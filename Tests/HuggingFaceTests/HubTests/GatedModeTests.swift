import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

@Suite("Gated Mode Tests")
struct GatedModeTests {
    @Test("GatedMode encodes correctly")
    func testGatedModeEncoding() throws {
        let encoder = JSONEncoder()

        let notGated = GatedMode.notGated
        let notGatedData = try encoder.encode(notGated)
        let notGatedString = String(data: notGatedData, encoding: .utf8)
        #expect(notGatedString == "false")

        let auto = GatedMode.auto
        let autoData = try encoder.encode(auto)
        let autoString = String(data: autoData, encoding: .utf8)
        #expect(autoString == "\"auto\"")

        let manual = GatedMode.manual
        let manualData = try encoder.encode(manual)
        let manualString = String(data: manualData, encoding: .utf8)
        #expect(manualString == "\"manual\"")
    }

    @Test("GatedMode decodes from boolean")
    func testGatedModeDecodingFromBool() throws {
        let decoder = JSONDecoder()

        let falseData = "false".data(using: .utf8)!
        let falseGated = try decoder.decode(GatedMode.self, from: falseData)
        #expect(falseGated == .notGated)

        let trueData = "true".data(using: .utf8)!
        let trueGated = try decoder.decode(GatedMode.self, from: trueData)
        #expect(trueGated == .auto)
    }

    @Test("GatedMode decodes from string")
    func testGatedModeDecodingFromString() throws {
        let decoder = JSONDecoder()

        let autoData = "\"auto\"".data(using: .utf8)!
        let autoGated = try decoder.decode(GatedMode.self, from: autoData)
        #expect(autoGated == .auto)

        let manualData = "\"manual\"".data(using: .utf8)!
        let manualGated = try decoder.decode(GatedMode.self, from: manualData)
        #expect(manualGated == .manual)
    }
}
