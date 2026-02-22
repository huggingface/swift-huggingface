import Foundation
import Testing

@testable import HuggingFace

@Suite("Expand List Tests")
struct ExpandListTests {
    @Test("Creates from string literal")
    func testStringLiteralInit() {
        let expand: ExpandList = "author, downloads, cardData"
        #expect(expand.contains("author"))
        #expect(expand.contains("downloads"))
        #expect(expand.contains("cardData"))
        #expect(expand.fields.count == 3)
    }

    @Test("Creates from array literal")
    func testArrayLiteralInit() {
        let expand: ExpandList = ["author", "downloadsAllTime", "likes"]
        #expect(expand.contains("author"))
        #expect(expand.contains("downloadsAllTime"))
        #expect(expand.contains("likes"))
    }

    @Test("Deduplicates and normalizes values")
    func testNormalizationAndDeduplication() {
        let expand = ExpandList(" author ,likes,author, ,downloads,")
        #expect(expand.fields == ["author", "downloads", "likes"])
    }

    @Test("Supports SetAlgebra operations")
    func testSetAlgebraOperations() {
        var base: ExpandList = ["author", "likes"]
        let extra: ExpandList = ["likes", "downloads"]

        let union = base.union(extra)
        #expect(union.fields == ["author", "downloads", "likes"])

        let intersection = base.intersection(extra)
        #expect(intersection.fields == ["likes"])

        let symmetric = base.symmetricDifference(extra)
        #expect(symmetric.fields == ["author", "downloads"])

        #expect(base.insert("downloads").inserted)
        #expect(base.contains("downloads"))

        #expect(base.remove("likes") == "likes")
        #expect(!base.contains("likes"))
    }
}
