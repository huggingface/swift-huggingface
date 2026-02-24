import Foundation
import Testing

#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Token Storage Tests", .serialized)
    struct TokenStorageTests {
        @Test("FileTokenStorage store retrieve delete lifecycle")
        func testFileTokenStorageLifecycle() throws {
            let (directory, fileURL) = makeTempTokenPath()
            defer { try? FileManager.default.removeItem(at: directory) }

            let storage = FileTokenStorage(fileURL: fileURL)
            let token = OAuthToken(
                accessToken: "access-token",
                refreshToken: "refresh-token",
                expiresAt: Date(timeIntervalSince1970: 1_700_000_000)
            )

            #expect(storage.hasStoredToken == false)
            try storage.store(token)
            #expect(storage.hasStoredToken == true)

            let retrieved = try #require(try storage.retrieve())
            #expect(retrieved.accessToken == token.accessToken)
            #expect(retrieved.refreshToken == token.refreshToken)
            #expect(retrieved.expiresAt == token.expiresAt)

            try storage.delete()
            #expect(storage.hasStoredToken == false)
            #expect(try storage.retrieve() == nil)
        }

        @Test("FileTokenStorage retrieve is nil when file is absent")
        func testFileTokenStorageRetrieveMissingFile() throws {
            let (directory, fileURL) = makeTempTokenPath()
            defer { try? FileManager.default.removeItem(at: directory) }

            let storage = FileTokenStorage(fileURL: fileURL)
            #expect(try storage.retrieve() == nil)
        }

        @Test("FileTokenStorage throws when file contains invalid JSON")
        func testFileTokenStorageInvalidJSONThrows() throws {
            let (directory, fileURL) = makeTempTokenPath()
            defer { try? FileManager.default.removeItem(at: directory) }

            try FileManager.default.createDirectory(
                at: fileURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try Data("{invalid-json".utf8).write(to: fileURL)

            let storage = FileTokenStorage(fileURL: fileURL)
            #expect(throws: DecodingError.self) {
                _ = try storage.retrieve()
            }
        }

        @Test("EnvironmentTokenStorage returns nil when variable is missing")
        func testEnvironmentTokenStorageMissingVariable() {
            let variable = "HF_TEST_TOKEN_\(UUID().uuidString.replacingOccurrences(of: "-", with: "_"))"
            unsetenv(variable)

            let storage = EnvironmentTokenStorage(variableName: variable)
            #expect(storage.retrieve() == nil)
        }

        @Test("EnvironmentTokenStorage reads token from environment")
        func testEnvironmentTokenStorageReadsVariable() {
            let variable = "HF_TEST_TOKEN_\(UUID().uuidString.replacingOccurrences(of: "-", with: "_"))"
            defer { unsetenv(variable) }
            setenv(variable, "env-access-token", 1)

            let storage = EnvironmentTokenStorage(variableName: variable)
            let token = storage.retrieve()
            #expect(token?.accessToken == "env-access-token")
            #expect(token?.refreshToken == nil)
            #expect(token?.expiresAt == Date.distantFuture)
        }

        @Test("CompositeTokenStorage prefers environment token over file token")
        func testCompositeStoragePrefersEnvironment() throws {
            let previousHFToken = ProcessInfo.processInfo.environment["HF_TOKEN"]
            defer { restoreEnvironment(name: "HF_TOKEN", previousValue: previousHFToken) }

            let (directory, fileURL) = makeTempTokenPath()
            defer { try? FileManager.default.removeItem(at: directory) }
            let fileStorage = FileTokenStorage(fileURL: fileURL)
            try fileStorage.store(
                OAuthToken(
                    accessToken: "file-token",
                    refreshToken: nil,
                    expiresAt: .distantFuture
                )
            )
            setenv("HF_TOKEN", "env-token", 1)

            let composite = CompositeTokenStorage(environment: true, file: fileStorage)
            let token = try composite.retrieve()
            #expect(token?.accessToken == "env-token")
        }

        @Test(
            "CompositeTokenStorage falls back to file token",
            .disabled(if: ProcessInfo.processInfo.environment["HF_TOKEN"]?.isEmpty == false)
        )
        func testCompositeStorageFallsBackToFile() throws {
            let previousHFToken = ProcessInfo.processInfo.environment["HF_TOKEN"]
            defer { restoreEnvironment(name: "HF_TOKEN", previousValue: previousHFToken) }
            unsetenv("HF_TOKEN")

            let (directory, fileURL) = makeTempTokenPath()
            defer { try? FileManager.default.removeItem(at: directory) }
            let fileStorage = FileTokenStorage(fileURL: fileURL)
            try fileStorage.store(
                OAuthToken(
                    accessToken: "file-token",
                    refreshToken: nil,
                    expiresAt: .distantFuture
                )
            )

            let composite = CompositeTokenStorage(environment: true, file: fileStorage)
            let token = try composite.retrieve()
            #expect(token?.accessToken == "file-token")
        }

        private func makeTempTokenPath() -> (URL, URL) {
            let directory = FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString, isDirectory: true)
            let fileURL = directory.appendingPathComponent("token.json")
            return (directory, fileURL)
        }

        private func restoreEnvironment(name: String, previousValue: String?) {
            if let previousValue {
                setenv(name, previousValue, 1)
            } else {
                unsetenv(name)
            }
        }
    }
#endif
