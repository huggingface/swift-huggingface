import Testing
import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
@testable import HuggingFace

@Suite("Git Tests")
struct GitTests {
    @Test("TreeEntry decoding")
    func testTreeEntryDecoding() throws {
        let json = """
            {
                "path": "README.md",
                "type": "file",
                "oid": "abc123def456",
                "size": 1234,
                "lastCommit": {
                    "id": "commit123",
                    "title": "Update README",
                    "date": "2023-01-15T10:30:00.000Z"
                }
            }
            """

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds

        let data = json.data(using: .utf8)!
        let entry = try decoder.decode(Git.TreeEntry.self, from: data)

        #expect(entry.path == "README.md")
        #expect(entry.type == .file)
        #expect(entry.oid == "abc123def456")
        #expect(entry.size == 1234)
        #expect(entry.lfs == nil)
        #expect(entry.effectiveSize == 1234)
        #expect(entry.lastCommit != nil)
        #expect(entry.lastCommit?.id == "commit123")
        #expect(entry.lastCommit?.title == "Update README")
    }

    @Test("TreeEntry directory decoding")
    func testTreeEntryDirectoryDecoding() throws {
        let json = """
            {
                "path": "src",
                "type": "directory",
                "oid": null,
                "size": null
            }
            """

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds

        let data = json.data(using: .utf8)!
        let entry = try decoder.decode(Git.TreeEntry.self, from: data)

        #expect(entry.path == "src")
        #expect(entry.type == .directory)
        #expect(entry.oid == nil)
        #expect(entry.size == nil)
        #expect(entry.effectiveSize == nil)
    }

    @Test("TreeEntry decodes LFS metadata and prefers its size")
    func testTreeEntryLFSDecoding() throws {
        let data = Data(
            #"{"path":"weights.bin","type":"file","size":128,"lfs":{"oid":"sha256","size":524288,"pointerSize":128}}"#
                .utf8
        )
        let entry = try JSONDecoder().decode(Git.TreeEntry.self, from: data)

        #expect(entry.size == 128)
        #expect(entry.lfs?.oid == "sha256")
        #expect(entry.lfs?.size == 524288)
        #expect(entry.lfs?.pointerSize == 128)
        #expect(entry.effectiveSize == 524288)
        let encoded = try JSONEncoder().encode(entry)
        #expect(try JSONDecoder().decode(Git.TreeEntry.self, from: encoded) == entry)
    }

    @Test("TreeEntry supports minimal and null LFS metadata")
    func testTreeEntryOptionalLFSMetadata() throws {
        let minimal = Data(#"{"path":"empty.bin","type":"file","lfs":{"size":0}}"#.utf8)
        let entry = try JSONDecoder().decode(Git.TreeEntry.self, from: minimal)
        #expect(entry.lfs?.oid == nil)
        #expect(entry.lfs?.pointerSize == nil)
        #expect(entry.effectiveSize == 0)

        let null = Data(#"{"path":"file.txt","type":"file","size":10,"lfs":null}"#.utf8)
        #expect(try JSONDecoder().decode(Git.TreeEntry.self, from: null).effectiveSize == 10)
    }

    // Captured on 2026-11-16 from
    // https://huggingface.co/api/models/bigscience/bloom/refs
    private static let modelRefsResponse = Data(
        #"{"tags":[{"name":"gs91100","ref":"refs/tags/gs91100","targetCommit":"d5f6a9a208dbb2de2ffe4fb27868f51ff776fea9"}],"branches":[{"name":"main","ref":"refs/heads/main","targetCommit":"7f10a99ce7c08f03c7719a586cb2cbda1433ac05"},{"name":"pr/121","ref":"refs/heads/pr/121","targetCommit":"46967aa0521973f212b461065132fb6e9a32c6d8"}],"converts":[]}"#
            .utf8
    )

    @Test("Ref decodes the target commit from a Hub response")
    func testRefDecoding() throws {
        let data = Data(
            #"{"name":"main","ref":"refs/heads/main","targetCommit":"7f10a99ce7c08f03c7719a586cb2cbda1433ac05"}"#
                .utf8
        )
        let ref = try JSONDecoder().decode(Git.Ref.self, from: data)

        #expect(ref.name == "main")
        #expect(ref.ref == "refs/heads/main")
        #expect(ref.targetCommit == "7f10a99ce7c08f03c7719a586cb2cbda1433ac05")
    }

    @Test("Ref keeps the deprecated targetOid property")
    @available(*, deprecated)
    func testRefDeprecatedTargetOid() throws {
        let data = Data(
            #"{"name":"main","ref":"refs/heads/main","targetCommit":"7f10a99ce7c08f03c7719a586cb2cbda1433ac05"}"#
                .utf8
        )
        let ref = try JSONDecoder().decode(Git.Ref.self, from: data)

        #expect(ref.targetOid == "7f10a99ce7c08f03c7719a586cb2cbda1433ac05")
    }

    @Test("Ref decodes values encoded with the legacy targetOid key")
    func testRefLegacyDecoding() throws {
        let data = Data(#"{"name":"main","ref":"refs/heads/main","targetOid":"abc123"}"#.utf8)
        let ref = try JSONDecoder().decode(Git.Ref.self, from: data)

        #expect(ref.targetCommit == "abc123")
    }

    @Test("Ref encodes targetCommit and round-trips")
    func testRefRoundTrip() throws {
        let data = Data(
            #"{"name":"gs91100","ref":"refs/tags/gs91100","targetCommit":"d5f6a9a208dbb2de2ffe4fb27868f51ff776fea9"}"#
                .utf8
        )
        let ref = try JSONDecoder().decode(Git.Ref.self, from: data)

        let encoded = try JSONEncoder().encode(ref)
        let object = try #require(JSONSerialization.jsonObject(with: encoded) as? [String: String])
        #expect(object["targetCommit"] == "d5f6a9a208dbb2de2ffe4fb27868f51ff776fea9")
        #expect(object["targetOid"] == nil)
        #expect(try JSONDecoder().decode(Git.Ref.self, from: encoded) == ref)
    }

    @Test("Refs list decoding")
    func testRefsListDecoding() throws {
        let decoded = try JSONDecoder().decode([String: [Git.Ref]].self, from: Self.modelRefsResponse)

        let branches = try #require(decoded["branches"])
        let tags = try #require(decoded["tags"])

        #expect(branches.map(\.name) == ["main", "pr/121"])
        #expect(branches.map(\.ref) == ["refs/heads/main", "refs/heads/pr/121"])
        #expect(
            branches.map(\.targetCommit) == [
                "7f10a99ce7c08f03c7719a586cb2cbda1433ac05",
                "46967aa0521973f212b461065132fb6e9a32c6d8",
            ]
        )
        #expect(tags.count == 1)
        #expect(tags[0].name == "gs91100")
        #expect(tags[0].ref == "refs/tags/gs91100")
        #expect(tags[0].targetCommit == "d5f6a9a208dbb2de2ffe4fb27868f51ff776fea9")
        #expect(decoded["converts"]?.isEmpty == true)
    }

    @Test("Commit decoding")
    func testCommitDecoding() throws {
        let json = """
            {
                "id": "commit123abc",
                "title": "Add new feature",
                "message": "Add new feature\\n\\nDetailed description here.",
                "date": "2023-06-15T14:30:00.000Z",
                "authors": [
                    {
                        "name": "John Doe",
                        "email": "john@example.com",
                        "time": "2023-06-15T14:30:00.000Z",
                        "user": "johndoe",
                        "avatarUrl": "https://avatars.example.com/johndoe"
                    }
                ],
                "parents": ["parent123abc"]
            }
            """

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds

        let data = json.data(using: .utf8)!
        let commit = try decoder.decode(Git.Commit.self, from: data)

        #expect(commit.id == "commit123abc")
        #expect(commit.title == "Add new feature")
        #expect(commit.message == "Add new feature\n\nDetailed description here.")
        #expect(commit.authors.count == 1)
        #expect(commit.authors[0].name == "John Doe")
        #expect(commit.authors[0].email == "john@example.com")
        #expect(commit.authors[0].user == "johndoe")
        #expect(commit.parents?.count == 1)
        #expect(commit.parents?[0] == "parent123abc")
    }

    @Test("Author decoding with minimal fields")
    func testAuthorMinimalDecoding() throws {
        let json = """
            {
                "name": "Jane Smith",
                "email": null,
                "time": null,
                "user": null,
                "avatarUrl": null
            }
            """

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds

        let data = json.data(using: .utf8)!
        let author = try decoder.decode(Git.Author.self, from: data)

        #expect(author.name == "Jane Smith")
        #expect(author.email == nil)
        #expect(author.time == nil)
        #expect(author.user == nil)
        #expect(author.avatarURL == nil)
    }
}
