import Foundation

/// Bridges serial Xet output updates and completes progress after cleanup succeeds.
enum XetDownloadProgress {
    static func track(
        _ progress: Progress?,
        operation: (_ report: @escaping @Sendable (Int64, Int64) -> Void) async throws -> Int64
    ) async throws {
        try Task.checkCancellation()
        progress?.completedUnitCount = 0
        let written = try await operation { completed, total in
            // Xet sends completion before its scoped downloader shuts down.
            // Reserve completion until the entire operation succeeds.
            if completed < total {
                progress?.totalUnitCount = total
                progress?.completedUnitCount = completed
            }
        }
        try Task.checkCancellation()
        progress?.totalUnitCount = written
        progress?.completedUnitCount = written
    }
}
