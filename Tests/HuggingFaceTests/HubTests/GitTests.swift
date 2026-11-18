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

    // Captured on 2026-11-17 from
    // https://huggingface.co/api/models/openai-community/gpt2/commits/main?limit=1
    private static let modelCommitsResponse = Data(
        #"[{"id":"607a30d783dfa663caf39e06633721c8d4cfcd7e","title":"Adds the tokenizer configuration file (#80)","message":"\n\n\n- Adds tokenizer_config.json file (db6d57930088fb63e52c010bd9ac77c955ac55e7)\n\n","authors":[{"user":"lysandre","avatar":"https://cdn-avatars.huggingface.co/v1/production/uploads/5e3aec01f55e2b62848a5217/PMKS0NNB4MJQlTSFzh918.jpeg"}],"date":"2024-02-19T10:57:45.000Z"}]"#
            .utf8
    )

    @Test("Commit decodes a Hub commits response")
    func testCommitDecoding() throws {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds

        let commits = try decoder.decode([Git.Commit].self, from: Self.modelCommitsResponse)

        #expect(commits.count == 1)
        let commit = try #require(commits.first)
        #expect(commit.id == "607a30d783dfa663caf39e06633721c8d4cfcd7e")
        #expect(commit.title == "Adds the tokenizer configuration file (#80)")
        #expect(
            commit.message
                == "\n\n\n- Adds tokenizer_config.json file (db6d57930088fb63e52c010bd9ac77c955ac55e7)\n\n"
        )
        #expect(commit.date == Date(timeIntervalSince1970: 1_708_340_265))
        #expect(commit.parents == nil)
        #expect(commit.authors.count == 1)
        #expect(commit.authors[0].user == "lysandre")
        #expect(commit.authors[0].name == "lysandre")
        #expect(commit.authors[0].email == nil)
        #expect(commit.authors[0].time == nil)
    }

    @Test("Author decodes the avatar URL from a Hub response")
    func testAuthorAvatarDecoding() throws {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds
        let commits = try decoder.decode([Git.Commit].self, from: Self.modelCommitsResponse)
        let author = try #require(commits.first?.authors.first)

        #expect(
            author.avatarURL
                == "https://cdn-avatars.huggingface.co/v1/production/uploads/5e3aec01f55e2b62848a5217/PMKS0NNB4MJQlTSFzh918.jpeg"
        )

        let encoded = try JSONEncoder().encode(author)
        let object = try #require(JSONSerialization.jsonObject(with: encoded) as? [String: Any])
        #expect(object["avatar"] as? String == author.avatarURL)
        #expect(object["avatarURL"] == nil)
    }

    @Test("Author round-trips and decodes values that earlier versions encoded")
    func testAuthorCoding() throws {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithFractionalSeconds
        let commits = try decoder.decode([Git.Commit].self, from: Self.modelCommitsResponse)
        let author = try #require(commits.first?.authors.first)
        let encoded = try JSONEncoder().encode(author)
        #expect(try JSONDecoder().decode(Git.Author.self, from: encoded) == author)

        // Encoded by this package at c4d1d40 with a default JSONEncoder.
        let legacy = Data(
            #"{"avatarURL":"https:\/\/example.com\/a.png","email":"john@example.com","name":"John Doe","time":708532200,"user":"johndoe"}"#
                .utf8
        )
        let legacyAuthor = try JSONDecoder().decode(Git.Author.self, from: legacy)
        #expect(legacyAuthor.name == "John Doe")
        #expect(legacyAuthor.email == "john@example.com")
        #expect(legacyAuthor.time == Date(timeIntervalSince1970: 1_686_839_400))
        #expect(legacyAuthor.user == "johndoe")
        #expect(legacyAuthor.avatarURL == "https://example.com/a.png")
    }

}
