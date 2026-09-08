import Foundation
import Testing

@testable import HuggingFace

@Suite("Tags")
struct TagsTests {
    /// The Hub answers `/api/models-tags-by-type` and `/api/datasets-tags-by-type`
    /// with the groups at the top level, keyed by type, and no wrapper.
    @Test("Decodes the Hub's tags-by-type shape")
    func decodesHubShape() throws {
        let json = """
            {
                "region": [
                    {"type": "region", "label": "Region: US", "id": "region:us"}
                ],
                "library": [
                    {"type": "library", "label": "PyTorch", "id": "pytorch"},
                    {"type": "library", "label": "Transformers", "id": "transformers"}
                ]
            }
            """

        let tags = try JSONDecoder().decode(Tags.self, from: Data(json.utf8))

        #expect(tags.count == 2)
        #expect(tags["region"]?.map(\.id) == ["region:us"])
        #expect(tags["library"]?.map(\.label) == ["PyTorch", "Transformers"])
    }

    @Test("Round-trips through JSON without a wrapper")
    func roundTrips() throws {
        let tags: Tags = [
            "library": [.init(id: "pytorch", label: "PyTorch", count: 42)]
        ]

        let data = try JSONEncoder().encode(tags)
        let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        #expect(object?.keys.sorted() == ["library"])

        let decoded = try JSONDecoder().decode(Tags.self, from: data)
        #expect(decoded["library"]?.first?.id == "pytorch")
        #expect(decoded["library"]?.first?.count == 42)
    }
}
