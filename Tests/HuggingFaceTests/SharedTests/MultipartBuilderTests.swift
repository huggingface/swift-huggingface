import Foundation
import Testing

@testable import HuggingFace

@Suite("Multipart Builder Tests")
struct MultipartBuilderTests {
    @Test("buildInMemory includes text and file parts")
    func testBuildInMemoryWithTextAndFile() throws {
        let (directory, fileURL) = try makeTempFile(named: "greeting.txt", contents: "hello file")
        defer { try? FileManager.default.removeItem(at: directory) }

        let boundary = "test-boundary"
        let data = try MultipartBuilder(boundary: boundary)
            .addText(name: "field", value: "value")
            .addFile(name: "upload", fileURL: fileURL, mimeType: "text/plain")
            .buildInMemory()

        let body = String(decoding: data, as: UTF8.self)
        #expect(body.contains("--\(boundary)\r\n"))
        #expect(body.contains("Content-Disposition: form-data; name=\"field\"\r\n"))
        #expect(body.contains("\r\nvalue\r\n"))
        #expect(body.contains("name=\"upload\"; filename=\"greeting.txt\""))
        #expect(body.contains("Content-Type: text/plain\r\n"))
        #expect(body.contains("hello file\r\n"))
        #expect(body.hasSuffix("--\(boundary)--\r\n"))
    }

    @Test("addOptionalText omits nil values")
    func testAddOptionalTextOmitsNil() throws {
        let boundary = "test-boundary"
        let data = try MultipartBuilder(boundary: boundary)
            .addOptionalText(name: "included", value: "present")
            .addOptionalText(name: "omitted", value: nil)
            .buildInMemory()

        let body = String(decoding: data, as: UTF8.self)
        #expect(body.contains("name=\"included\""))
        #expect(!body.contains("name=\"omitted\""))
    }

    @Test("buildToTempFile matches buildInMemory output")
    func testBuildToTempFileMatchesInMemory() throws {
        let (directory, fileURL) = try makeTempFile(named: "payload.txt", contents: "stream me")
        defer { try? FileManager.default.removeItem(at: directory) }

        let builder = MultipartBuilder(boundary: "stable-boundary")
            .addText(name: "alpha", value: "beta")
            .addFileStreamed(name: "file", fileURL: fileURL, mimeType: nil)

        let inMemory = try builder.buildInMemory()
        let tempFile = try builder.buildToTempFile()
        defer { try? FileManager.default.removeItem(at: tempFile) }
        let streamed = try Data(contentsOf: tempFile)

        #expect(inMemory == streamed)
    }

    #if !canImport(FoundationNetworking)
        @Test("buildToTempFile throws for non-file URL input")
        func testBuildToTempFileInvalidStreamURL() throws {
            let builder = MultipartBuilder(boundary: "test-boundary")
                .addFileStreamed(
                    name: "file",
                    fileURL: URL(string: "https://example.com/file.txt")!,
                    mimeType: "text/plain"
                )

            #expect(throws: MultipartBuilderError.self) {
                _ = try builder.buildToTempFile()
            }
        }
    #endif  // !canImport(FoundationNetworking)

    private func makeTempFile(named: String, contents: String) throws -> (URL, URL) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let fileURL = directory.appendingPathComponent(named)
        try Data(contents.utf8).write(to: fileURL)
        return (directory, fileURL)
    }
}
