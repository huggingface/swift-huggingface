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
    }

    @Test("Ref decoding")
    func testRefDecoding() throws {
        let json = """
            {
                "name": "main",
                "ref": "refs/heads/main",
                "targetOid": "abc123def456"
            }
            """

        let data = json.data(using: .utf8)!
        let ref = try JSONDecoder().decode(Git.Ref.self, from: data)

        #expect(ref.name == "main")
        #expect(ref.ref == "refs/heads/main")
        #expect(ref.targetOid == "abc123def456")
    }

    @Test("Refs list decoding")
    func testRefsListDecoding() throws {
        let json = """
            {
                "branches": [
                    {
                        "name": "main",
                        "ref": "refs/heads/main",
                        "targetOid": "abc123"
                    },
                    {
                        "name": "dev",
                        "ref": "refs/heads/dev",
                        "targetOid": "def456"
                    }
                ],
                "tags": [
                    {
                        "name": "v1.0.0",
                        "ref": "refs/tags/v1.0.0",
                        "targetOid": "xyz789"
                    }
                ]
            }
            """

        let data = json.data(using: .utf8)!
        let decoded = try JSONDecoder().decode([String: [Git.Ref]?].self, from: data)

        let branches = decoded["branches"] ?? []
        let tags = decoded["tags"]

        #expect(branches?.count == 2)
        #expect(tags??.count == 1)
        #expect(branches?[0].name == "main")
        #expect(branches?[1].name == "dev")
        #expect(tags??[0].name == "v1.0.0")

        // Test utility of creating a dictionary by name
        let branchesByName = Dictionary(uniqueKeysWithValues: branches?.map { ($0.name, $0) } ?? [])
        #expect(branchesByName["main"]?.name == "main")
        #expect(branchesByName["dev"]?.name == "dev")
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
