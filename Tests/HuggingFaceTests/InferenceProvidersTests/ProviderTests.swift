import Foundation
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Provider Tests")
    struct ProviderTests {
        @Test("Built-in provider identifiers map to API values")
        func testBuiltInProviderIdentifiers() {
            #expect(Provider.auto.identifier == "auto")
            #expect(Provider.falAI.identifier == "fal-ai")
            #expect(Provider.fireworks.identifier == "fireworks-ai")
            #expect(Provider.hfInference.identifier == "hf-inference")
            #expect(Provider.zai.identifier == "zai-org")
        }

        @Test("Display names are human readable")
        func testDisplayNames() {
            #expect(Provider.auto.displayName == "Auto")
            #expect(Provider.hfInference.displayName == "Hugging Face Inference")
            #expect(Provider.sambaNova.displayName == "SambaNova")
            #expect(Provider.custom(name: "my-provider").displayName == "my-provider")
        }

        @Test("Capabilities vary by provider")
        func testCapabilities() {
            #expect(Provider.cerebras.capabilities == [.chatCompletion])
            #expect(Provider.publicAI.capabilities == [.chatCompletion])
            #expect(
                Provider.hfInference.capabilities
                    == [.chatCompletion, .chatCompletionVLM, .featureExtraction, .textToImage, .textToVideo]
            )
        }

        @Test("Auto and custom providers expose all capabilities")
        func testAutoAndCustomCapabilities() {
            #expect(Provider.auto.capabilities == Set(Capability.allCases))
            #expect(Provider.custom(name: "custom").capabilities == Set(Capability.allCases))
        }

        @Test("Decode built-in provider from string")
        func testDecodeBuiltInProviderFromString() throws {
            let decoded = try JSONDecoder().decode(Provider.self, from: Data(#""hf-inference""#.utf8))
            #expect(decoded == .hfInference)
        }

        @Test("Decode unknown provider string as custom provider")
        func testDecodeUnknownStringAsCustomProvider() throws {
            let decoded = try JSONDecoder().decode(Provider.self, from: Data(#""my-provider""#.utf8))
            #expect(decoded == .custom(name: "my-provider", baseURL: nil))
        }

        @Test("Decode custom provider object with base URL")
        func testDecodeCustomProviderObject() throws {
            let json = """
                {
                  "name": "internal-gateway",
                  "baseURL": "https://example.com/inference"
                }
                """
            let decoded = try JSONDecoder().decode(Provider.self, from: Data(json.utf8))
            #expect(
                decoded
                    == .custom(
                        name: "internal-gateway",
                        baseURL: URL(string: "https://example.com/inference")
                    )
            )
        }

        @Test("Encode built-in provider as string")
        func testEncodeBuiltInAsString() throws {
            let data = try JSONEncoder().encode(Provider.groq)
            let string = String(decoding: data, as: UTF8.self)
            #expect(string == #""groq""#)
        }

        @Test("Every built-in provider round-trips through its identifier")
        func testBuiltInProvidersRoundTrip() throws {
            #expect(Provider.builtIn.count == 18)
            #expect(Set(Provider.builtIn.map(\.identifier)).count == Provider.builtIn.count)
            for provider in Provider.builtIn {
                let data = try JSONEncoder().encode(provider)
                #expect(String(decoding: data, as: UTF8.self) == "\"\(provider.identifier)\"")
                #expect(try JSONDecoder().decode(Provider.self, from: data) == provider)
            }
        }

        @Test("A custom provider isn't equal to a built-in one with the same identifier")
        func testCustomIsNotBuiltIn() {
            #expect(Provider.custom(name: "groq") != .groq)
            #expect(Provider.custom(name: "groq") == .custom(name: "groq"))
            #expect(Provider.custom(name: "x", baseURL: URL(string: "https://example.com")) != .custom(name: "x"))
        }

        @Test("Capabilities encode and decode as their identifiers")
        func testCapabilityCoding() throws {
            #expect(Capability.allCases.count == 22)
            #expect(Set(Capability.allCases.map(\.rawValue)).count == Capability.allCases.count)
            let data = try JSONEncoder().encode([Capability.textToImage, Capability(rawValue: "image_to_3d")])
            #expect(String(decoding: data, as: UTF8.self) == #"["text_to_image","image_to_3d"]"#)
            let decoded = try JSONDecoder().decode([Capability].self, from: data)
            #expect(decoded == [.textToImage, Capability(rawValue: "image_to_3d")])
        }

        @Test("Encode custom provider as object")
        func testEncodeCustomAsObject() throws {
            let provider = Provider.custom(name: "internal", baseURL: URL(string: "https://example.com")!)
            let data = try JSONEncoder().encode(provider)
            let json = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
            #expect(json["name"] as? String == "internal")
            #expect(json["baseURL"] as? String == "https://example.com")
        }
    }
#endif
