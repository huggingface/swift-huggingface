#if HUGGINGFACE_ENABLE_XET

    import Foundation
    import Testing

    @testable import HuggingFace

    @Suite("Bucket Types")
    struct BucketTests {
        // MARK: - Bucket.ID

        @Test("Bucket.ID parses namespace/name form")
        func parseValidID() {
            let id = Bucket.ID(rawValue: "user/my-bucket")
            #expect(id?.namespace == "user")
            #expect(id?.name == "my-bucket")
            #expect(id?.rawValue == "user/my-bucket")
        }

        @Test("Bucket.ID rejects malformed input", arguments: [
            "no-slash",
            "",
        ])
        func parseInvalidID(_ raw: String) {
            #expect(Bucket.ID(rawValue: raw) == nil)
        }

        // MARK: - Bucket.Region

        @Test("Bucket.Region has the listed regions and accepts others")
        func regions() {
            #expect(Bucket.Region.us.rawValue == "us")
            #expect(Bucket.Region.eu.rawValue == "eu")
            #expect(Bucket.Region(rawValue: "us") == .us)
            #expect(Bucket.Region(rawValue: "ap").rawValue == "ap")
            #expect(Bucket.Region.allCases == [.us, .eu])
        }

        @Test("Bucket.Region encodes and decodes as its identifier")
        func regionCoding() throws {
            let encoded = try JSONEncoder().encode([Bucket.Region.eu, Bucket.Region(rawValue: "ap")])
            #expect(String(decoding: encoded, as: UTF8.self) == #"["eu","ap"]"#)
            let decoded = try JSONDecoder().decode([Bucket.Region].self, from: encoded)
            #expect(decoded == [.eu, Bucket.Region(rawValue: "ap")])
        }

        // MARK: - Bucket

        @Test("Bucket decodes the Hub's shape")
        func decodeBucket() throws {
            let payload = Data(
                """
                {
                  "id": "user/my-bucket",
                  "private": true,
                  "createdAt": "2026-02-06T17:37:57.123Z",
                  "size": 551879671,
                  "totalFiles": 12
                }
                """.utf8)

            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds

            let bucket = try decoder.decode(Bucket.self, from: payload)
            #expect(bucket.id.rawValue == "user/my-bucket")
            #expect(bucket.visibility?.isPrivate == true)
            #expect(bucket.size == 551_879_671)
            #expect(bucket.totalFiles == 12)
        }

        // MARK: - Bucket.TreeEntry (discriminated union)

        @Test("Bucket.TreeEntry decodes file entries")
        func decodeFileEntry() throws {
            let payload = Data(
                """
                {
                  "type": "file",
                  "path": "checkpoints/model.safetensors",
                  "size": 2408828,
                  "xetHash": "3ed0e9fefe788ddd61d1e26eba67057e9740a064b009256fbafadf6bb95785ca",
                  "mtime": "2024-09-25T15:31:02.346Z",
                  "uploadedAt": "2024-09-25T15:31:05.000Z"
                }
                """.utf8)

            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds

            let entry = try decoder.decode(Bucket.TreeEntry.self, from: payload)
            guard case .file(let file) = entry else {
                Issue.record("Expected .file, got \(entry)")
                return
            }
            #expect(file.path == "checkpoints/model.safetensors")
            #expect(file.size == 2_408_828)
            #expect(file.xetHash.hasPrefix("3ed0e9fe"))
            #expect(file.mtime != nil)
        }

        @Test("Bucket.TreeEntry decodes directory entries")
        func decodeDirectoryEntry() throws {
            let payload = Data(
                """
                {
                  "type": "directory",
                  "path": "checkpoints",
                  "uploadedAt": "2024-09-25T15:31:05.000Z"
                }
                """.utf8)

            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds

            let entry = try decoder.decode(Bucket.TreeEntry.self, from: payload)
            guard case .folder(let folder) = entry else {
                Issue.record("Expected .folder, got \(entry)")
                return
            }
            #expect(folder.path == "checkpoints")
            #expect(folder.uploadedAt != nil)
        }

        @Test("Bucket.TreeEntry rejects unknown discriminator")
        func decodeUnknownType() {
            let payload = Data(#"{"type": "unknown", "path": "x"}"#.utf8)
            #expect(throws: DecodingError.self) {
                _ = try JSONDecoder().decode(Bucket.TreeEntry.self, from: payload)
            }
        }
    }

#endif  // HUGGINGFACE_ENABLE_XET
