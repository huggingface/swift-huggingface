import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking

    // MARK: - URLSession Async Extensions for Linux

    /// Provides async/await wrappers for URLSession APIs that are missing on Linux.
    /// These extensions bridge the callback-based APIs to Swift's concurrency model.
    extension URLSession {
        /// Performs an HTTP request and returns the response data.
        ///
        /// This is a compatibility shim for Linux where the native async `data(for:)` may not be available.
        ///
        /// - Parameter request: The URL request to perform.
        /// - Returns: A tuple containing the response data and URL response.
        func asyncData(for request: URLRequest) async throws -> (Data, URLResponse) {
            try await withCheckedThrowingContinuation { continuation in
                let task = self.dataTask(with: request) { data, response, error in
                    if let error = error {
                        continuation.resume(throwing: error)
                        return
                    }
                    guard let data = data, let response = response else {
                        continuation.resume(
                            throwing: URLError(.badServerResponse)
                        )
                        return
                    }
                    continuation.resume(returning: (data, response))
                }
                task.resume()
            }
        }

        /// Uploads data to a URL and returns the response.
        ///
        /// - Parameters:
        ///   - request: The URL request to perform.
        ///   - data: The data to upload.
        /// - Returns: A tuple containing the response data and URL response.
        func asyncUpload(for request: URLRequest, from data: Data) async throws -> (Data, URLResponse) {
            try await withCheckedThrowingContinuation { continuation in
                let task = self.uploadTask(with: request, from: data) { data, response, error in
                    if let error = error {
                        continuation.resume(throwing: error)
                        return
                    }
                    guard let data = data, let response = response else {
                        continuation.resume(
                            throwing: URLError(.badServerResponse)
                        )
                        return
                    }
                    continuation.resume(returning: (data, response))
                }
                task.resume()
            }
        }

        /// Uploads a file to a URL and returns the response.
        ///
        /// - Parameters:
        ///   - request: The URL request to perform.
        ///   - fileURL: The URL of the file to upload.
        /// - Returns: A tuple containing the response data and URL response.
        func asyncUpload(for request: URLRequest, fromFile fileURL: URL) async throws -> (Data, URLResponse)
        {
            try await withCheckedThrowingContinuation { continuation in
                let task = self.uploadTask(with: request, fromFile: fileURL) { data, response, error in
                    if let error = error {
                        continuation.resume(throwing: error)
                        return
                    }
                    guard let data = data, let response = response else {
                        continuation.resume(
                            throwing: URLError(.badServerResponse)
                        )
                        return
                    }
                    continuation.resume(returning: (data, response))
                }
                task.resume()
            }
        }

        /// Downloads a file from a URL to a temporary location.
        ///
        /// - Parameters:
        ///   - request: The URL request to perform.
        ///   - progress: Optional progress object to track download progress.
        /// - Returns: A tuple containing the temporary file URL and URL response.
        func asyncDownload(
            for request: URLRequest,
            progress: Progress? = nil
        ) async throws -> (URL, URLResponse) {
            try await withCheckedThrowingContinuation { continuation in
                let delegate = progress.map { LinuxDownloadDelegate(progress: $0, continuation: continuation) }

                if let delegate = delegate {
                    // Use delegate-based download for progress tracking
                    let session = URLSession(
                        configuration: self.configuration,
                        delegate: delegate,
                        delegateQueue: nil
                    )
                    let task = session.downloadTask(with: request)
                    delegate.task = task
                    task.resume()
                } else {
                    // Simple download without progress
                    let task = self.downloadTask(with: request) { url, response, error in
                        if let error = error {
                            continuation.resume(throwing: error)
                            return
                        }
                        guard let tempURL = url, let response = response else {
                            continuation.resume(throwing: URLError(.badServerResponse))
                            return
                        }
                        // Copy to a new temp location since the original will be deleted
                        let newTempURL = FileManager.default.temporaryDirectory
                            .appendingPathComponent(UUID().uuidString)
                        do {
                            try FileManager.default.copyItem(at: tempURL, to: newTempURL)
                            continuation.resume(returning: (newTempURL, response))
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                    task.resume()
                }
            }
        }

        /// Streams bytes from a URL request.
        ///
        /// This provides a simplified streaming interface for Linux where `bytes(for:)` is not available.
        /// Note: This implementation buffers the entire response, so it's not suitable for very large responses.
        /// For true streaming on Linux, consider using a different HTTP client library.
        ///
        /// - Parameter request: The URL request to perform.
        /// - Returns: A tuple containing the response bytes and URL response.
        func asyncBytes(for request: URLRequest) async throws -> (LinuxAsyncBytes, URLResponse) {
            let (data, response) = try await asyncData(for: request)
            return (LinuxAsyncBytes(data: data), response)
        }
    }

    // MARK: - Linux Download Delegate

    /// A delegate for tracking download progress on Linux.
    private final class LinuxDownloadDelegate: NSObject, URLSessionDownloadDelegate, @unchecked Sendable {
        let progress: Progress
        let continuation: CheckedContinuation<(URL, URLResponse), Error>
        var task: URLSessionDownloadTask?
        private var hasResumed = false

        init(progress: Progress, continuation: CheckedContinuation<(URL, URLResponse), Error>) {
            self.progress = progress
            self.continuation = continuation
        }

        func urlSession(
            _ session: URLSession,
            downloadTask: URLSessionDownloadTask,
            didWriteData bytesWritten: Int64,
            totalBytesWritten: Int64,
            totalBytesExpectedToWrite: Int64
        ) {
            progress.totalUnitCount = totalBytesExpectedToWrite
            progress.completedUnitCount = totalBytesWritten
        }

        func urlSession(
            _ session: URLSession,
            downloadTask: URLSessionDownloadTask,
            didFinishDownloadingTo location: URL
        ) {
            guard !hasResumed else { return }
            hasResumed = true

            guard let response = downloadTask.response else {
                continuation.resume(throwing: URLError(.badServerResponse))
                return
            }

            // Copy to a new temp location since the original will be deleted
            let newTempURL = FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString)
            do {
                try FileManager.default.copyItem(at: location, to: newTempURL)
                continuation.resume(returning: (newTempURL, response))
            } catch {
                continuation.resume(throwing: error)
            }
        }

        func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
            guard !hasResumed else { return }
            hasResumed = true

            if let error = error {
                continuation.resume(throwing: error)
            }
            // If no error and not resumed, the download delegate method should have handled it
        }
    }

    // MARK: - Linux Async Bytes

    /// A simple async sequence wrapper for bytes on Linux.
    /// This is a simplified implementation that works with pre-loaded data.
    struct LinuxAsyncBytes: AsyncSequence, Sendable {
        typealias Element = UInt8

        let data: Data

        struct AsyncIterator: AsyncIteratorProtocol {
            var index: Data.Index
            let endIndex: Data.Index
            let data: Data

            mutating func next() async -> UInt8? {
                guard index < endIndex else { return nil }
                let byte = data[index]
                index = data.index(after: index)
                return byte
            }
        }

        func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(index: data.startIndex, endIndex: data.endIndex, data: data)
        }
    }

#endif
