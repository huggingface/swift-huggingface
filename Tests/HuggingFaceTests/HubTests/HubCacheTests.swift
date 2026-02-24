import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

@Suite("HubCache Tests")
struct HubCacheTests {
    let tempDirectory: URL

    init() throws {
        tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("HubCacheTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )
    }

    // MARK: - Repository Directory Tests

    @Test("Repository directory path for model")
    func repoDirectoryModel() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "openai-community/gpt2"

        let repoDir = cache.repoDirectory(repo: repoID, kind: .model)

        #expect(repoDir.lastPathComponent == "models--openai-community--gpt2")
        #expect(repoDir.deletingLastPathComponent().path == tempDirectory.path)
    }

    @Test("Repository directory path for dataset")
    func repoDirectoryDataset() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "squad_v2/squad_v2"

        let repoDir = cache.repoDirectory(repo: repoID, kind: .dataset)

        #expect(repoDir.lastPathComponent == "datasets--squad_v2--squad_v2")
    }

    @Test("Repository directory path for space")
    func repoDirectorySpace() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "huggingface/gradio-demo"

        let repoDir = cache.repoDirectory(repo: repoID, kind: .space)

        #expect(repoDir.lastPathComponent == "spaces--huggingface--gradio-demo")
    }

    // MARK: - Subdirectory Tests

    @Test("Blobs directory path")
    func blobsDirectory() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"

        let blobsDir = cache.blobsDirectory(repo: repoID, kind: .model)

        #expect(blobsDir.lastPathComponent == "blobs")
        #expect(blobsDir.deletingLastPathComponent().lastPathComponent == "models--user--repo")
    }

    @Test("Refs directory path")
    func refsDirectory() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"

        let refsDir = cache.refsDirectory(repo: repoID, kind: .model)

        #expect(refsDir.lastPathComponent == "refs")
        #expect(refsDir.deletingLastPathComponent().lastPathComponent == "models--user--repo")
    }

    @Test("Snapshots directory path")
    func snapshotsDirectory() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"

        let snapshotsDir = cache.snapshotsDirectory(repo: repoID, kind: .model)

        #expect(snapshotsDir.lastPathComponent == "snapshots")
        #expect(snapshotsDir.deletingLastPathComponent().lastPathComponent == "models--user--repo")
    }

    @Test("Lock path uses .locks hierarchy")
    func lockPathUsesLocksHierarchy() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let blobPath = cache.blobsDirectory(repo: repoID, kind: .model).appendingPathComponent("etag123")

        let lockPath = cache.lockPath(for: blobPath).appendingPathExtension("lock")
        let expected =
            tempDirectory
            .appendingPathComponent(".locks")
            .appendingPathComponent("models--user--repo")
            .appendingPathComponent("blobs")
            .appendingPathComponent("etag123.lock")

        #expect(lockPath == expected)
    }

    // MARK: - Ref Resolution Tests

    @Test("Resolve revision from ref file")
    func resolveRevision() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        // Create refs directory and file
        let refsDir = cache.refsDirectory(repo: repoID, kind: .model)
        try FileManager.default.createDirectory(at: refsDir, withIntermediateDirectories: true)
        let refFile = refsDir.appendingPathComponent("main")
        try commitHash.write(to: refFile, atomically: true, encoding: .utf8)

        let resolved = cache.resolveRevision(repo: repoID, kind: .model, ref: "main")

        #expect(resolved == commitHash)
    }

    @Test("Resolve revision returns nil for missing ref")
    func resolveRevisionMissing() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"

        let resolved = cache.resolveRevision(repo: repoID, kind: .model, ref: "nonexistent")

        #expect(resolved == nil)
    }

    @Test("Resolve revision trims whitespace")
    func resolveRevisionTrimsWhitespace() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        // Create refs directory and file with trailing newline
        let refsDir = cache.refsDirectory(repo: repoID, kind: .model)
        try FileManager.default.createDirectory(at: refsDir, withIntermediateDirectories: true)
        let refFile = refsDir.appendingPathComponent("main")
        try "\(commitHash)\n".write(to: refFile, atomically: true, encoding: .utf8)

        let resolved = cache.resolveRevision(repo: repoID, kind: .model, ref: "main")

        #expect(resolved == commitHash)
    }

    // MARK: - Update Ref Tests

    @Test("Update ref creates ref file")
    func updateRef() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        try cache.updateRef(repo: repoID, kind: .model, ref: "main", commit: commitHash)

        let refFile = cache.refsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent("main")
        let contents = try String(contentsOf: refFile, encoding: .utf8)
        #expect(contents == commitHash)
    }

    @Test("Update ref overwrites existing ref")
    func updateRefOverwrite() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let oldCommit = "0000000000000000000000000000000000000000"
        let newCommit = "1111111111111111111111111111111111111111"

        try cache.updateRef(repo: repoID, kind: .model, ref: "main", commit: oldCommit)
        try cache.updateRef(repo: repoID, kind: .model, ref: "main", commit: newCommit)

        let resolved = cache.resolveRevision(repo: repoID, kind: .model, ref: "main")
        #expect(resolved == newCommit)
    }

    @Test("Update ref handles nested refs like refs/pr/5")
    func updateRefNestedPath() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        try cache.updateRef(repo: repoID, kind: .model, ref: "refs/pr/5", commit: commitHash)

        let resolved = cache.resolveRevision(repo: repoID, kind: .model, ref: "refs/pr/5")
        #expect(resolved == commitHash)
    }

    // MARK: - Etag Normalization Tests

    @Test("Normalize etag removes quotes")
    func normalizeEtagQuotes() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)

        let normalized = cache.normalizeEtag("\"abc123\"")

        #expect(normalized == "abc123")
    }

    @Test("Normalize etag removes weak validator prefix")
    func normalizeEtagWeakPrefix() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)

        let normalized = cache.normalizeEtag("W/\"abc123\"")

        #expect(normalized == "abc123")
    }

    @Test("Normalize etag handles plain etag")
    func normalizeEtagPlain() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)

        let normalized = cache.normalizeEtag("abc123")

        #expect(normalized == "abc123")
    }

    // MARK: - Cache Lookup Tests

    @Test("Cached file path returns existing file")
    func cachedFilePathExists() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let filename = "config.json"

        // Create snapshots directory and file
        let snapshotsDir = cache.snapshotsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent(commitHash)
        try FileManager.default.createDirectory(at: snapshotsDir, withIntermediateDirectories: true)
        let filePath = snapshotsDir.appendingPathComponent(filename)
        try "{}".write(to: filePath, atomically: true, encoding: .utf8)

        let cachedPath = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: filename
        )

        #expect(cachedPath != nil)
        #expect(cachedPath == filePath)
    }

    @Test("Cached file path resolves branch name")
    func cachedFilePathResolvesBranch() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let filename = "config.json"

        // Create ref file
        try cache.updateRef(repo: repoID, kind: .model, ref: "main", commit: commitHash)

        // Create snapshots directory and file
        let snapshotsDir = cache.snapshotsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent(commitHash)
        try FileManager.default.createDirectory(at: snapshotsDir, withIntermediateDirectories: true)
        let filePath = snapshotsDir.appendingPathComponent(filename)
        try "{}".write(to: filePath, atomically: true, encoding: .utf8)

        let cachedPath = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: "main",
            filename: filename
        )

        #expect(cachedPath != nil)
        #expect(cachedPath == filePath)
    }

    @Test("Cached file path returns nil for missing file")
    func cachedFilePathMissing() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let cachedPath = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: "nonexistent.txt"
        )

        #expect(cachedPath == nil)
    }

    @Test("Cached file path returns nil for unresolvable revision")
    func cachedFilePathUnresolvableRevision() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"

        let cachedPath = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: "unknown-branch",
            filename: "config.json"
        )

        #expect(cachedPath == nil)
    }

    // MARK: - Cached Blob Tests

    @Test("Cached blob path returns existing blob")
    func cachedBlobPathExists() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let etag = "abc123def456"

        // Create blobs directory and file
        let blobsDir = cache.blobsDirectory(repo: repoID, kind: .model)
        try FileManager.default.createDirectory(at: blobsDir, withIntermediateDirectories: true)
        let blobPath = blobsDir.appendingPathComponent(etag)
        try "content".write(to: blobPath, atomically: true, encoding: .utf8)

        let cachedPath = cache.cachedBlobPath(repo: repoID, kind: .model, etag: etag)

        #expect(cachedPath != nil)
        #expect(cachedPath == blobPath)
    }

    @Test("Cached blob path normalizes etag")
    func cachedBlobPathNormalizesEtag() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let rawEtag = "\"abc123def456\""
        let normalizedEtag = "abc123def456"

        // Create blobs directory and file with normalized name
        let blobsDir = cache.blobsDirectory(repo: repoID, kind: .model)
        try FileManager.default.createDirectory(at: blobsDir, withIntermediateDirectories: true)
        let blobPath = blobsDir.appendingPathComponent(normalizedEtag)
        try "content".write(to: blobPath, atomically: true, encoding: .utf8)

        let cachedPath = cache.cachedBlobPath(repo: repoID, kind: .model, etag: rawEtag)

        #expect(cachedPath != nil)
        #expect(cachedPath == blobPath)
    }

    @Test("Cached blob path returns nil for missing blob")
    func cachedBlobPathMissing() throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"

        let cachedPath = cache.cachedBlobPath(repo: repoID, kind: .model, etag: "nonexistent")

        #expect(cachedPath == nil)
    }

    // MARK: - Store File Tests

    @Test("Store file creates blob and snapshot symlink")
    func storeFile() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let filename = "config.json"
        let etag = "etag123"
        let content = "{ \"key\": \"value\" }"

        // Create source file
        let sourceFile = tempDirectory.appendingPathComponent("source.json")
        try content.write(to: sourceFile, atomically: true, encoding: .utf8)

        try await cache.storeFile(
            at: sourceFile,
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: filename,
            etag: etag
        )

        // Verify blob was created
        let blobPath = cache.blobsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent(etag)
        #expect(FileManager.default.fileExists(atPath: blobPath.path))

        // Verify snapshot entry was created
        let snapshotPath = cache.snapshotsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent(commitHash)
            .appendingPathComponent(filename)
        #expect(FileManager.default.fileExists(atPath: snapshotPath.path))

        // Verify content is accessible through snapshot
        let storedContent = try String(contentsOf: snapshotPath, encoding: .utf8)
        #expect(storedContent == content)
    }

    @Test("Store file updates ref when provided")
    func storeFileUpdatesRef() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        // Create source file
        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        try await cache.storeFile(
            at: sourceFile,
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: "file.txt",
            etag: "etag123",
            ref: "main"
        )

        let resolved = cache.resolveRevision(repo: repoID, kind: .model, ref: "main")
        #expect(resolved == commitHash)
    }

    @Test("Store file handles nested paths")
    func storeFileNestedPath() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let filename = "path/to/nested/config.json"
        let etag = "etag123"

        // Create source file
        let sourceFile = tempDirectory.appendingPathComponent("source.json")
        try "{}".write(to: sourceFile, atomically: true, encoding: .utf8)

        try await cache.storeFile(
            at: sourceFile,
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: filename,
            etag: etag
        )

        // Verify nested snapshot entry was created
        let snapshotPath = cache.snapshotsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent(commitHash)
            .appendingPathComponent(filename)
        #expect(FileManager.default.fileExists(atPath: snapshotPath.path))
    }

    @Test("Store file does not duplicate blob")
    func storeFileNoDuplicateBlob() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commit1 = "1111111111111111111111111111111111111111"
        let commit2 = "2222222222222222222222222222222222222222"
        let etag = "shared-etag"
        let content = "shared content"

        // Create source files
        let sourceFile1 = tempDirectory.appendingPathComponent("source1.txt")
        let sourceFile2 = tempDirectory.appendingPathComponent("source2.txt")
        try content.write(to: sourceFile1, atomically: true, encoding: .utf8)
        try content.write(to: sourceFile2, atomically: true, encoding: .utf8)

        // Store same content in two different revisions
        try await cache.storeFile(
            at: sourceFile1,
            repo: repoID,
            kind: .model,
            revision: commit1,
            filename: "file.txt",
            etag: etag
        )

        try await cache.storeFile(
            at: sourceFile2,
            repo: repoID,
            kind: .model,
            revision: commit2,
            filename: "file.txt",
            etag: etag
        )

        // Both snapshots should work
        let cachedPath1 = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: commit1,
            filename: "file.txt"
        )
        let cachedPath2 = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: commit2,
            filename: "file.txt"
        )

        #expect(cachedPath1 != nil)
        #expect(cachedPath2 != nil)

        // Verify only one blob exists (excluding .lock files)
        let blobsDir = cache.blobsDirectory(repo: repoID, kind: .model)
        let allFiles = try FileManager.default.contentsOfDirectory(atPath: blobsDir.path)
        let blobs = allFiles.filter { !$0.hasSuffix(".lock") }
        #expect(blobs.count == 1)
        #expect(blobs.first == etag)
    }

    // MARK: - Store Data Tests

    @Test("Store data creates blob and snapshot")
    func storeData() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let filename = "config.json"
        let etag = "etag123"
        let content = "{ \"key\": \"value\" }"
        let data = Data(content.utf8)

        try await cache.storeData(
            data,
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: filename,
            etag: etag
        )

        // Verify blob was created
        let blobPath = cache.blobsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent(etag)
        #expect(FileManager.default.fileExists(atPath: blobPath.path))

        // Verify content is accessible
        let cachedPath = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: filename
        )
        #expect(cachedPath != nil)

        let storedContent = try String(contentsOf: cachedPath!, encoding: .utf8)
        #expect(storedContent == content)
    }

    @Test("Store data updates ref when provided")
    func storeDataUpdatesRef() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        try await cache.storeData(
            Data("content".utf8),
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: "file.txt",
            etag: "etag123",
            ref: "main"
        )

        let resolved = cache.resolveRevision(repo: repoID, kind: .model, ref: "main")
        #expect(resolved == commitHash)
    }

    // MARK: - Default Cache Tests

    @Test("Default cache uses environment-based location")
    func defaultCache() throws {
        let cache = HubCache.default

        // The default cache should use environment-based detection
        #expect(cache.cacheDirectory.path.contains("huggingface"))
    }

    // MARK: - Location Provider Tests

    @Test("Cache with fixed location provider")
    func cacheWithFixedLocation() throws {
        let customDir = tempDirectory.appendingPathComponent("custom-cache")
        let cache = HubCache(location: .fixed(directory: customDir))

        #expect(cache.cacheDirectory == customDir)
    }

    @Test("Cache with path string location")
    func cacheWithPathLocation() throws {
        let cache = HubCache(location: .init(path: tempDirectory.path))

        #expect(cache.cacheDirectory.path == tempDirectory.path)
    }

    // MARK: - Path Traversal Validation Tests

    @Test("Store file rejects etag with path traversal")
    func storeFileRejectsEtagPathTraversal() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "file.txt",
                etag: "../../../etc/passwd"
            )
        }
    }

    @Test("Store file rejects revision with path traversal")
    func storeFileRejectsRevisionPathTraversal() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: "../../../.ssh/authorized_keys",
                filename: "file.txt",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store file rejects etag with forward slash")
    func storeFileRejectsEtagWithSlash() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "file.txt",
                etag: "path/to/file"
            )
        }
    }

    @Test("Store file rejects etag with backslash")
    func storeFileRejectsEtagWithBackslash() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "file.txt",
                etag: "path\\to\\file"
            )
        }
    }

    @Test("Store file rejects empty etag")
    func storeFileRejectsEmptyEtag() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "file.txt",
                etag: ""
            )
        }
    }

    @Test("Store file rejects etag with null byte")
    func storeFileRejectsEtagWithNullByte() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "file.txt",
                etag: "etag\0injected"
            )
        }
    }

    @Test("Store data rejects etag with path traversal")
    func storeDataRejectsEtagPathTraversal() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        await #expect(throws: HubCacheError.self) {
            try await cache.storeData(
                Data("content".utf8),
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "file.txt",
                etag: "..\\..\\windows\\system32"
            )
        }
    }

    @Test("Store data rejects revision with path traversal")
    func storeDataRejectsRevisionPathTraversal() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"

        await #expect(throws: HubCacheError.self) {
            try await cache.storeData(
                Data("content".utf8),
                repo: repoID,
                kind: .model,
                revision: "..%2F..%2F..",
                filename: "file.txt",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store file accepts valid etag and revision")
    func storeFileAcceptsValidComponents() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let etag = "abc123def456-valid_etag.with"  // Valid chars including dots and hyphens

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        // Should not throw
        try await cache.storeFile(
            at: sourceFile,
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: "file.txt",
            etag: etag
        )

        let cachedPath = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: "file.txt"
        )
        #expect(cachedPath != nil)
    }

    // MARK: - Filename Path Traversal Validation Tests

    @Test("Store file rejects filename with path traversal")
    func storeFileRejectsFilenamePathTraversal() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "../../../.ssh/authorized_keys",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store file rejects filename with embedded path traversal")
    func storeFileRejectsFilenameEmbeddedTraversal() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "models/../../../etc/passwd",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store file rejects filename with absolute path")
    func storeFileRejectsFilenameAbsolutePath() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "/etc/passwd",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store file rejects filename with backslash")
    func storeFileRejectsFilenameBackslash() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "..\\..\\windows\\system32\\config",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store file rejects empty filename")
    func storeFileRejectsEmptyFilename() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store file rejects filename with null byte")
    func storeFileRejectsFilenameNullByte() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "config\0.json",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store file rejects filename with empty path component")
    func storeFileRejectsFilenameEmptyComponent() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        await #expect(throws: HubCacheError.self) {
            try await cache.storeFile(
                at: sourceFile,
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "path//to/file.txt",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store data rejects filename with path traversal")
    func storeDataRejectsFilenamePathTraversal() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"

        await #expect(throws: HubCacheError.self) {
            try await cache.storeData(
                Data("content".utf8),
                repo: repoID,
                kind: .model,
                revision: commitHash,
                filename: "../../.ssh/authorized_keys",
                etag: "valid-etag"
            )
        }
    }

    @Test("Store file accepts valid nested filename")
    func storeFileAcceptsValidNestedFilename() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let filename = "tokenizer/vocab.json"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        try await cache.storeFile(
            at: sourceFile,
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: filename,
            etag: "valid-etag"
        )

        let cachedPath = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: filename
        )
        #expect(cachedPath != nil)
    }

    @Test("Store file accepts deeply nested filename")
    func storeFileAcceptsDeeplyNestedFilename() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let filename = "path/to/deeply/nested/config.json"

        let sourceFile = tempDirectory.appendingPathComponent("source.txt")
        try "content".write(to: sourceFile, atomically: true, encoding: .utf8)

        try await cache.storeFile(
            at: sourceFile,
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: filename,
            etag: "valid-etag"
        )

        let cachedPath = cache.cachedFilePath(
            repo: repoID,
            kind: .model,
            revision: commitHash,
            filename: filename
        )
        #expect(cachedPath != nil)
    }
}
