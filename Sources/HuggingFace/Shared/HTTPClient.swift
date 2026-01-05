import Foundation
import EventSource

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

/// HTTP methods supported by the client.
enum HTTPMethod: String, Hashable, Sendable {
    case get = "GET"
    case post = "POST"
    case put = "PUT"
    case delete = "DELETE"
    case patch = "PATCH"
    case head = "HEAD"
}

// MARK: - Rate Limit Parsing

/// Parses rate limit reset time from HTTP response headers.
///
/// Follows IETF draft: https://www.ietf.org/archive/id/draft-ietf-httpapi-ratelimit-headers-09.html
/// Also falls back to standard Retry-After header.
///
/// - Parameter response: The HTTP response to parse.
/// - Returns: The number of seconds until the rate limit resets, or nil if not found.
func parseRateLimitResetSeconds(from response: HTTPURLResponse) -> Double? {
    // First, try IETF draft ratelimit header: "api";r=0;t=55
    // We need the 't' value (seconds until reset)
    if let ratelimit = response.value(forHTTPHeaderField: "ratelimit")
        ?? response.value(forHTTPHeaderField: "RateLimit")
    {
        // Parse format: "resource_type";r=remaining;t=reset_seconds
        // Split on semicolons and find part starting with "t="
        for part in ratelimit.split(separator: ";") {
            let trimmed = part.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("t="), let seconds = Double(String(trimmed.dropFirst(2))) {
                return seconds
            }
        }
    }

    // Fall back to standard Retry-After header
    if let retryAfter = response.value(forHTTPHeaderField: "Retry-After"),
        let seconds = Double(retryAfter)
    {
        return seconds
    }

    return nil
}

// MARK: - Retry Configuration

/// Configuration for HTTP request retry behavior with exponential backoff.
/// Matches huggingface_hub's http_backoff behavior.
struct RetryConfiguration: Sendable {
    /// Maximum number of retries before giving up.
    let maxRetries: Int

    /// Initial wait time in seconds before first retry.
    let baseWaitTime: TimeInterval

    /// Maximum wait time in seconds between retries.
    let maxWaitTime: TimeInterval

    /// HTTP status codes that trigger a retry.
    let retryOnStatusCodes: Set<Int>

    /// Default configuration matching huggingface_hub.
    static let `default` = RetryConfiguration(
        maxRetries: 5,
        baseWaitTime: 1.0,
        maxWaitTime: 8.0,
        retryOnStatusCodes: [429, 500, 502, 503, 504]
    )

    /// No retries - used for first page requests where we want immediate failure feedback.
    /// Subsequent pages use `.default` retry to handle transient failures during pagination.
    static let none = RetryConfiguration(
        maxRetries: 0,
        baseWaitTime: 0,
        maxWaitTime: 0,
        retryOnStatusCodes: []
    )
}

/// Base HTTP client with common functionality for all Hugging Face API clients.
final class HTTPClient: @unchecked Sendable {
    /// The host URL for requests.
    let host: URL

    /// The value for the `User-Agent` header sent in requests, if any.
    let userAgent: String?

    /// The token provider for authentication.
    let tokenProvider: TokenProvider

    /// The underlying URL session.
    let session: URLSession

    /// A shared JSON decoder with consistent configuration.
    private let jsonDecoder: JSONDecoder = {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds
        return decoder
    }()

    /// Creates a new HTTP client.
    /// - Parameters:
    ///   - host: The host URL for requests.
    ///   - userAgent: The value for the `User-Agent` header sent in requests.
    ///   - tokenProvider: The token provider for authentication.
    ///   - session: The underlying URL session.
    init(
        host: URL,
        userAgent: String? = nil,
        tokenProvider: TokenProvider = .environment,
        session: URLSession = URLSession(configuration: .default)
    ) {
        var host = host
        if !host.path.hasSuffix("/") {
            host = host.appendingPathComponent("")
        }

        self.host = host
        self.userAgent = userAgent
        self.tokenProvider = tokenProvider
        self.session = session
    }

    private struct ClientErrorResponse: Decodable {
        let error: String
    }

    // MARK: - Request Methods

    func fetch<T: Decodable>(
        _ method: HTTPMethod,
        _ path: String,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) async throws -> T {
        let request = try await createRequest(method, path, params: params, headers: headers)

        return try await performFetch(request: request)
    }

    func fetch<T: Decodable>(
        _ method: HTTPMethod,
        url: URL,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) async throws -> T {
        let request = try await createRequest(method, url: url, params: params, headers: headers)
        return try await performFetch(request: request)
    }

    private func performFetch<T: Decodable>(request: URLRequest) async throws -> T {
        let (data, response) = try await session.data(for: request)
        let httpResponse = try validateResponse(response, data: data)

        if T.self == Bool.self {
            // If T is Bool, we return true for successful response
            return true as! T
        } else if data.isEmpty {
            throw HTTPClientError.responseError(response: httpResponse, detail: "Empty response body")
        } else {
            do {
                return try jsonDecoder.decode(T.self, from: data)
            } catch {
                throw HTTPClientError.decodingError(
                    response: httpResponse,
                    detail: "Error decoding response: \(error.localizedDescription)"
                )
            }
        }
    }

    func fetchPaginated<T: Decodable>(
        _ method: HTTPMethod,
        _ path: String,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) async throws -> PaginatedResponse<T> {
        let request = try await createRequest(method, path, params: params, headers: headers)
        return try await performFetchPaginated(request: request, retry: .none)
    }

    func fetchPaginated<T: Decodable>(
        _ method: HTTPMethod,
        url: URL,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil,
        retry: RetryConfiguration = .default
    ) async throws -> PaginatedResponse<T> {
        let request = try await createRequest(method, url: url, params: params, headers: headers)
        return try await performFetchPaginated(request: request, retry: retry)
    }

    private func performFetchPaginated<T: Decodable>(
        request: URLRequest,
        retry: RetryConfiguration
    ) async throws -> PaginatedResponse<T> {
        var lastError: Error?
        var sleepTime = retry.baseWaitTime

        for attempt in 0 ... retry.maxRetries {
            try Task.checkCancellation()
            do {
                let (data, response) = try await session.data(for: request)

                guard let httpResponse = response as? HTTPURLResponse else {
                    throw HTTPClientError.unexpectedError("Invalid HTTP response")
                }

                // Check if we should retry based on status code
                if retry.retryOnStatusCodes.contains(httpResponse.statusCode), attempt < retry.maxRetries {
                    // Retry on these status codes
                    lastError = HTTPClientError.responseError(
                        response: httpResponse,
                        detail: "HTTP \(httpResponse.statusCode)"
                    )

                    // For rate limiting (429), check for rate limit headers
                    let waitTime: TimeInterval =
                        if httpResponse.statusCode == 429,
                            let seconds = parseRateLimitResetSeconds(from: httpResponse)
                        {
                            seconds + 1  // +1s to avoid rounding issues
                        } else {
                            sleepTime
                        }

                    try await Task.sleep(for: .seconds(waitTime))
                    sleepTime = min(retry.maxWaitTime, sleepTime * 2)  // Exponential backoff
                    continue
                }

                let validated = try validateResponse(response, data: data)

                do {
                    let items = try jsonDecoder.decode([T].self, from: data)
                    let nextURL = parseNextPageURL(from: validated)
                    return PaginatedResponse(items: items, nextURL: nextURL)
                } catch {
                    throw HTTPClientError.decodingError(
                        response: validated,
                        detail: "Error decoding response: \(error.localizedDescription)"
                    )
                }
            } catch let error as HTTPClientError {
                // HTTPClientError is thrown after validation - the retry check already happened
                throw error
            } catch {
                // Retry on network errors (timeout, connection issues)
                if attempt < retry.maxRetries {
                    lastError = error
                    try await Task.sleep(for: .seconds(sleepTime))
                    sleepTime = min(retry.maxWaitTime, sleepTime * 2)
                    continue
                }
                throw error
            }
        }

        throw lastError ?? HTTPClientError.unexpectedError("Retry failed")
    }

    func fetchStream<T: Decodable & Sendable>(
        _ method: HTTPMethod,
        _ path: String,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) -> AsyncThrowingStream<T, Swift.Error> {
        performFetchStream(
            method,
            requestBuilder: { [self] in
                try await self.createRequest(method, path, params: params, headers: headers)
            }
        )
    }

    func fetchStream<T: Decodable & Sendable>(
        _ method: HTTPMethod,
        url: URL,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) -> AsyncThrowingStream<T, Swift.Error> {
        performFetchStream(
            method,
            requestBuilder: { [self] in
                try await self.createRequest(method, url: url, params: params, headers: headers)
            }
        )
    }

    private func performFetchStream<T: Decodable & Sendable>(
        _ method: HTTPMethod,
        requestBuilder: @escaping @Sendable () async throws -> URLRequest
    ) -> AsyncThrowingStream<T, Swift.Error> {
        AsyncThrowingStream { @Sendable continuation in
            let task = Task {
                do {
                    let request = try await requestBuilder()

                    #if canImport(FoundationNetworking)
                        // Linux: Use buffered approach since true streaming is not available
                        let (data, response) = try await session.data(for: request)
                        let httpResponse = try validateResponse(response, data: data)

                        guard (200 ..< 300).contains(httpResponse.statusCode) else {
                            return
                        }

                        // Parse SSE events from the buffered response
                        guard let responseString = String(data: data, encoding: .utf8) else {
                            continuation.finish()
                            return
                        }

                        for line in responseString.components(separatedBy: "\n") {
                            let trimmed = line.trimmingCharacters(in: .whitespaces)
                            guard trimmed.hasPrefix("data:") else { continue }

                            let eventData = String(trimmed.dropFirst(5)).trimmingCharacters(
                                in: .whitespaces
                            )

                            // Check for [DONE] signal
                            if eventData == "[DONE]" {
                                continuation.finish()
                                return
                            }

                            guard let jsonData = eventData.data(using: .utf8) else {
                                continue
                            }

                            do {
                                let decoded = try jsonDecoder.decode(T.self, from: jsonData)
                                continuation.yield(decoded)
                            } catch {
                                print("Warning: Failed to decode streaming response chunk: \(error)")
                            }
                        }

                        continuation.finish()
                    #else
                        // Apple platforms: Use native streaming APIs
                        let (bytes, response) = try await session.bytes(for: request)
                        let httpResponse = try validateResponse(response)

                        guard (200 ..< 300).contains(httpResponse.statusCode) else {
                            var errorData = Data()
                            for try await byte in bytes {
                                errorData.append(byte)
                            }
                            // validateResponse will throw the appropriate error
                            _ = try validateResponse(response, data: errorData)
                            return  // This line will never be reached, but satisfies the compiler
                        }

                        for try await event in bytes.events {
                            // Check for [DONE] signal
                            if event.data.trimmingCharacters(in: .whitespacesAndNewlines) == "[DONE]" {
                                continuation.finish()
                                return
                            }

                            guard let jsonData = event.data.data(using: .utf8) else {
                                continue
                            }

                            do {
                                let decoded = try jsonDecoder.decode(T.self, from: jsonData)
                                continuation.yield(decoded)
                            } catch {
                                // Log decoding errors but don't fail the stream
                                // This allows the stream to continue even if individual chunks fail
                                print("Warning: Failed to decode streaming response chunk: \(error)")
                            }
                        }

                        continuation.finish()
                    #endif
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }

    func fetchData(
        _ method: HTTPMethod,
        _ path: String,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) async throws -> Data {
        let request = try await createRequest(method, path, params: params, headers: headers)
        return try await performFetchData(request: request)
    }

    func fetchData(
        _ method: HTTPMethod,
        url: URL,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) async throws -> Data {
        let request = try await createRequest(method, url: url, params: params, headers: headers)
        return try await performFetchData(request: request)
    }

    private func performFetchData(request: URLRequest) async throws -> Data {
        let (data, response) = try await session.data(for: request)
        let _ = try validateResponse(response, data: data)

        return data
    }

    func createRequest(
        _ method: HTTPMethod,
        _ path: String,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) async throws -> URLRequest {
        var urlComponents = URLComponents(url: host, resolvingAgainstBaseURL: true)
        urlComponents?.path = path

        return try await createRequest(method, urlComponents: urlComponents, params: params, headers: headers)
    }

    func createRequest(
        _ method: HTTPMethod,
        url: URL,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) async throws -> URLRequest {
        let urlComponents = URLComponents(url: url, resolvingAgainstBaseURL: true)

        return try await createRequest(method, urlComponents: urlComponents, params: params, headers: headers)
    }

    private func createRequest(
        _ method: HTTPMethod,
        urlComponents: URLComponents?,
        params: [String: Value]? = nil,
        headers: [String: String]? = nil
    ) async throws -> URLRequest {
        var urlComponents = urlComponents

        var httpBody: Data? = nil
        switch method {
        case .get, .head:
            if let params {
                var queryItems: [URLQueryItem] = []
                for (key, value) in params {
                    queryItems.append(URLQueryItem(name: key, value: value.description))
                }
                urlComponents?.queryItems = queryItems
            }
        case .post, .put, .delete, .patch:
            if let params {
                let encoder = JSONEncoder()
                // Special-case: allow sending a top-level JSON value (e.g., array)
                // by passing a single empty-string key.
                // This is used for endpoints that require an array body instead of an object.
                if params.count == 1, let sole = params.first, sole.key == "" {
                    httpBody = try encoder.encode(sole.value)
                } else {
                    httpBody = try encoder.encode(params)
                }
            }
        }

        guard let url = urlComponents?.url else {
            throw HTTPClientError.requestError(
                #"Unable to construct URL from components \#(String(describing: urlComponents))"#
            )
        }
        var request: URLRequest = URLRequest(url: url)
        request.httpMethod = method.rawValue

        request.addValue("application/json", forHTTPHeaderField: "Accept")
        if let userAgent {
            request.addValue(userAgent, forHTTPHeaderField: "User-Agent")
        }

        // Get authentication token from provider
        if let token = try await tokenProvider.getToken() {
            request.addValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        if let httpBody {
            request.httpBody = httpBody
            request.addValue("application/json", forHTTPHeaderField: "Content-Type")
        }

        // Add custom headers
        if let headers {
            for (key, value) in headers {
                request.addValue(value, forHTTPHeaderField: key)
            }
        }

        return request
    }

    func validateResponse(_ response: URLResponse, data: Data? = nil) throws -> HTTPURLResponse {
        guard let httpResponse = response as? HTTPURLResponse else {
            throw HTTPClientError.unexpectedError("Invalid response from server: \(response)")
        }

        // If we have data and it's an error status, parse and throw the error
        if let data = data, !(200 ..< 300).contains(httpResponse.statusCode) {
            if let errorDetail = try? jsonDecoder.decode(ClientErrorResponse.self, from: data) {
                throw HTTPClientError.responseError(response: httpResponse, detail: errorDetail.error)
            }

            if let string = String(data: data, encoding: .utf8) {
                throw HTTPClientError.responseError(response: httpResponse, detail: string)
            }

            throw HTTPClientError.responseError(response: httpResponse, detail: "Invalid response")
        }

        return httpResponse
    }
}

/// Represents errors that can occur during API operations.
public enum HTTPClientError: Error, Hashable, Sendable, CustomStringConvertible {
    /// An error encountered while constructing the request.
    case requestError(String)

    /// An error returned by the HTTP API.
    case responseError(response: HTTPURLResponse, detail: String)

    /// An error encountered while decoding the response.
    case decodingError(response: HTTPURLResponse, detail: String)

    /// An unexpected error.
    case unexpectedError(String)

    public var description: String {
        switch self {
        case .requestError(let detail):
            return "Request error: \(detail)"
        case .responseError(let response, let detail):
            return "Response error (Status \(response.statusCode)): \(detail)"
        case .decodingError(let response, let detail):
            return "Decoding error (Status \(response.statusCode)): \(detail)"
        case .unexpectedError(let detail):
            return "Unexpected error: \(detail)"
        }
    }
}
