import Foundation
import Testing

@testable import HuggingFace

@Suite("Xet output progress")
struct XetDownloadProgressTests {
    enum Failure: Error { case transfer, shutdown }

    @Test func reportsOutputBytesAndWaitsForCleanup() async throws {
        let progress = Progress(totalUnitCount: 100)
        progress.completedUnitCount = 100
        try await XetDownloadProgress.track(progress) { report in
            #expect(progress.completedUnitCount == 0)
            report(0, 524_288)
            report(131_072, 524_288)
            #expect(progress.totalUnitCount == 524_288)
            #expect(progress.completedUnitCount == 131_072)
            #expect(progress.fractionCompleted == 0.25)
            report(524_288, 524_288)
            #expect(!progress.isFinished)
            return 524_288
        }
        #expect(progress.totalUnitCount == 524_288)
        #expect(progress.completedUnitCount == 524_288)
    }

    @Test(arguments: [Int64(0), 3])
    func completesShortFiles(size: Int64) async throws {
        let parent = Progress(totalUnitCount: 1)
        let progress = Progress(totalUnitCount: 100, parent: parent, pendingUnitCount: 1)
        try await XetDownloadProgress.track(progress) { report in
            report(size, size)
            #expect(!progress.isFinished)
            #expect(!parent.isFinished)
            return size
        }
        #expect(progress.totalUnitCount == max(size, 1))
        #expect(progress.completedUnitCount == max(size, 1))
        #expect(progress.isFinished)
        #expect(progress.fractionCompleted == 1)
        #expect(parent.isFinished)
        #expect(parent.fractionCompleted == 1)
    }

    @Test(arguments: [Failure.transfer, .shutdown])
    func failureDoesNotComplete(failure: Failure) async {
        let progress = Progress(totalUnitCount: 0)
        await #expect(throws: Failure.self) {
            try await XetDownloadProgress.track(progress) { report in
                report(12, 48)
                if failure == .shutdown { report(48, 48) }
                throw failure
            }
        }
        #expect(progress.completedUnitCount == 12)
        #expect(progress.totalUnitCount == 48)
        #expect(!progress.isFinished)
    }

    @Test func cancellationDoesNotComplete() async {
        let progress = Progress(totalUnitCount: 0)
        let task = Task {
            try await XetDownloadProgress.track(progress) { report in
                report(12, 48)
                withUnsafeCurrentTask { $0?.cancel() }
                report(48, 48)
                return 48
            }
        }
        await #expect(throws: CancellationError.self) { try await task.value }
        #expect(progress.completedUnitCount == 12)
        #expect(!progress.isFinished)
    }

    @Test func concurrentFilesKeepSeparateCounts() async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            for size in [Int64(64), 128] {
                group.addTask {
                    let progress = Progress(totalUnitCount: 0)
                    try await XetDownloadProgress.track(progress) { report in
                        report(size / 2, size)
                        await Task.yield()
                        #expect(progress.completedUnitCount == size / 2)
                        #expect(progress.totalUnitCount == size)
                        return size
                    }
                    #expect(progress.completedUnitCount == size)
                }
            }
            try await group.waitForAll()
        }
    }
}
