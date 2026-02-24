import Foundation
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Message Codable Tests")
    struct MessageCodableTests {
        @Test("Content text round-trip encodes as string")
        func testContentTextRoundTrip() throws {
            let message = Message(role: .user, content: .text("hello"))
            let data = try JSONEncoder().encode(message)
            let json = String(decoding: data, as: UTF8.self)

            #expect(json.contains("\"content\":\"hello\""))

            let decoded = try JSONDecoder().decode(Message.self, from: data)
            #expect(decoded.role == .user)
            #expect(decoded.content == .text("hello"))
        }

        @Test("Mixed content round-trip preserves text and image detail")
        func testMixedContentRoundTrip() throws {
            let message = Message(
                role: .user,
                content: .mixed([
                    .text("look at this"),
                    .image(url: "https://example.com/image.jpg", detail: .high),
                ])
            )
            let data = try JSONEncoder().encode(message)
            let decoded = try JSONDecoder().decode(Message.self, from: data)

            #expect(decoded.content == message.content)
        }

        @Test("Image item defaults detail to auto when omitted")
        func testImageDetailDefaultsToAuto() throws {
            let json = """
                {
                  "role": "user",
                  "content": [
                    {
                      "type": "image_url",
                      "image_url": {
                        "url": "https://example.com/cat.jpg"
                      }
                    }
                  ]
                }
                """
            let data = Data(json.utf8)
            let decoded = try JSONDecoder().decode(Message.self, from: data)

            #expect(
                decoded.content
                    == .mixed([
                        .image(url: "https://example.com/cat.jpg", detail: .auto)
                    ])
            )
        }

        @Test("Decoding fails for invalid image content type")
        func testInvalidImageTypeThrows() {
            let json = """
                {
                  "role": "user",
                  "content": [
                    {
                      "type": "not_image",
                      "image_url": {
                        "url": "https://example.com/cat.jpg"
                      }
                    }
                  ]
                }
                """

            #expect(throws: DecodingError.self) {
                _ = try JSONDecoder().decode(Message.self, from: Data(json.utf8))
            }
        }

        @Test("Decoding fails for invalid detail value")
        func testInvalidDetailThrows() {
            let json = """
                {
                  "role": "user",
                  "content": [
                    {
                      "type": "image_url",
                      "image_url": {
                        "url": "https://example.com/cat.jpg",
                        "detail": "extreme"
                      }
                    }
                  ]
                }
                """

            #expect(throws: DecodingError.self) {
                _ = try JSONDecoder().decode(Message.self, from: Data(json.utf8))
            }
        }

        @Test("Message decodes nil content when missing or explicit null")
        func testMessageDecodeNilContent() throws {
            let missingContentJSON = """
                {
                  "role": "assistant"
                }
                """
            let explicitNullJSON = """
                {
                  "role": "assistant",
                  "content": null
                }
                """

            let missing = try JSONDecoder().decode(Message.self, from: Data(missingContentJSON.utf8))
            let explicitNull = try JSONDecoder().decode(Message.self, from: Data(explicitNullJSON.utf8))

            #expect(missing.content == nil)
            #expect(explicitNull.content == nil)
            #expect(missing.role == .assistant)
            #expect(explicitNull.role == .assistant)
        }
    }
#endif
