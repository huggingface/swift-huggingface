import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

@Suite("Repo.ID Tests")
struct RepoIDTests {
    @Test("Repo.ID parses namespace and name correctly")
    func testRepoIDParsing() {
        let repoID = Repo.ID(rawValue: "facebook/bart-large-cnn")
        #expect(repoID != nil)
        #expect(repoID?.namespace == "facebook")
        #expect(repoID?.name == "bart-large-cnn")
    }

    @Test("Repo.ID string literal initialization")
    func testRepoIDStringLiteral() {
        let repoID: Repo.ID = "huggingface/transformers"
        #expect(repoID.namespace == "huggingface")
        #expect(repoID.name == "transformers")
    }

    @Test("Repo.ID raw value representation")
    func testRepoIDRawValue() {
        let repoID = Repo.ID(namespace: "microsoft", name: "phi-2")
        #expect(repoID.rawValue == "microsoft/phi-2")
    }

    @Test("Repo.ID equality comparison")
    func testRepoIDEquality() {
        let repoID1: Repo.ID = "openai/gpt-3"
        let repoID2 = Repo.ID(namespace: "openai", name: "gpt-3")
        #expect(repoID1 == repoID2)
    }

    @Test("Repo.ID description")
    func testRepoIDDescription() {
        let repoID: Repo.ID = "google/bert"
        #expect(repoID.description == "google/bert")
    }

    @Test("Repo.ID fails to parse invalid format")
    func testRepoIDInvalidFormat() {
        let invalidRepoID = Repo.ID(rawValue: "invalid")
        #expect(invalidRepoID == nil)
    }
}
