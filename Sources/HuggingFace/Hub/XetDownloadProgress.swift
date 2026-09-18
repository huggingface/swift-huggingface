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
        // Foundation needs a positive total to finish empty files and their parents.
        let total = max(written, 1)
        progress?.totalUnitCount = total
        progress?.completedUnitCount = total
    }
}
