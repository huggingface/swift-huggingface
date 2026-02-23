import Foundation
import Testing

@testable import HuggingFace

@Suite("Comma Separated List Tests")
struct CommaSeparatedListTests {
    @Test("Creates from string literal")
    func testStringLiteralInit() {
        let values: CommaSeparatedList<String> = "author, downloads, cardData"
        #expect(values.contains("author"))
        #expect(values.contains("downloads"))
        #expect(values.contains("cardData"))
        #expect(values.fields.count == 3)
    }

    @Test("Creates from array literal")
    func testArrayLiteralInit() {
        let values: CommaSeparatedList<String> = ["author", "downloadsAllTime", "likes"]
        #expect(values.contains("author"))
        #expect(values.contains("downloadsAllTime"))
        #expect(values.contains("likes"))
    }

    @Test("Deduplicates and normalizes values")
    func testNormalizationAndDeduplication() {
        let values = CommaSeparatedList<String>(" author ,likes,author, ,downloads,")
        #expect(values.fields == ["author", "downloads", "likes"])
    }

    @Test("RawRepresentable round-trip")
    func testRawRepresentableRoundTrip() {
        let value = "likes,author,downloads"
        let values = CommaSeparatedList<String>(rawValue: value)
        #expect(values.contains("likes"))
        #expect(values.contains("author"))
        #expect(values.contains("downloads"))
        #expect(CommaSeparatedList<String>(rawValue: values.rawValue) == values)
    }

    @Test("Supports SetAlgebra operations")
    func testSetAlgebraOperations() {
        var base: CommaSeparatedList<String> = ["author", "likes"]
        let extra: CommaSeparatedList<String> = ["likes", "downloads"]

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

    @Test("Supports enum values and all constructor")
    func testEnumValuesAndAll() {
        let all = CommaSeparatedList<HubClient.ModelInference>.all
        #expect(all.contains(.warm))
        #expect(all.rawValue == "warm")

        let parsed = CommaSeparatedList<HubClient.ModelInference>(rawValue: "warm,unknown,warm")
        #expect(parsed == all)

        let modelFields = CommaSeparatedList<HubClient.ModelExpandField>.all
        #expect(modelFields.contains(.author))
        #expect(modelFields.contains(.safetensors))
    }
}
