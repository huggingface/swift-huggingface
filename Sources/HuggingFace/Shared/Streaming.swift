import Foundation
import EventSource

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

final class StreamingDelegate: NSObject, URLSessionDataDelegate {
    private let continuation: AsyncThrowingStream<EventSource.Event, Error>.Continuation
    private let parser = EventSource.Parser()
    private let onResponse: @Sendable (HTTPURLResponse) throws -> Void

    init(
        continuation: AsyncThrowingStream<EventSource.Event, Error>.Continuation,
        onResponse: @escaping @Sendable (HTTPURLResponse) throws -> Void
    ) {
        self.continuation = continuation
        self.onResponse = onResponse
    }

    func urlSession(
        _ session: URLSession,
        dataTask: URLSessionDataTask,
        didReceive response: URLResponse,
        completionHandler: @escaping @Sendable (URLSession.ResponseDisposition) -> Void
    ) {
        guard
            let http = response
                as? HTTPURLResponse /*,
           (200 ..< 300).contains(http.statusCode),
            http.value(forHTTPHeaderField: "Content-Type")?.lowercased().hasPrefix("text/event-stream") == true*/
        else {
            completionHandler(.cancel)
            continuation.finish(throwing: URLError(.badServerResponse))
            return
        }

        do {
            try onResponse(http)
            completionHandler(.allow)
        } catch {
            completionHandler(.cancel)
            continuation.finish(throwing: error)
        }
    }

    func urlSession(
        _ session: URLSession,
        dataTask: URLSessionDataTask,
        didReceive data: Data
    ) {
        Task {
            for byte in data {
                await parser.consume(byte)
                while let event = await parser.getNextEvent() {
                    continuation.yield(event)
                }
            }
        }
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: Error?
    ) {
        Task {
            await parser.finish()
            if let event = await parser.getNextEvent() {
                continuation.yield(event)
            }
            if let error { continuation.finish(throwing: error) } else { continuation.finish() }
        }
    }
}

func streamEvents(
    request: URLRequest,
    configuration: URLSessionConfiguration = .default,
    onResponse: @escaping @Sendable (HTTPURLResponse) throws -> Void
)
    -> AsyncThrowingStream<EventSource.Event, Error>
{
    AsyncThrowingStream { continuation in
        let delegate = StreamingDelegate(
            continuation: continuation,
            onResponse: onResponse
        )
        let session = URLSession(
            configuration: configuration,
            delegate: delegate,
            delegateQueue: nil
        )
        let task = session.dataTask(with: request)
        task.resume()

        continuation.onTermination = { _ in
            task.cancel()
            session.invalidateAndCancel()
        }
    }
}
