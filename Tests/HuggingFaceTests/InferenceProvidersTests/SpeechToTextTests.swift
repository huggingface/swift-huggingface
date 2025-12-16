import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

#if swift(>=6.1)

    /// Tests for the Speech to Text API
    @Suite("Speech to Text Tests", .serialized)
    struct SpeechToTextTests {
        /// Helper to create a URL session with mock protocol handlers
        func createMockClient() -> InferenceClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return InferenceClient(
                session: session,
                host: URL(string: "https://router.huggingface.co")!,
                userAgent: "TestClient/1.0"
            )
        }

        @Test("Basic speech to text transcription", .mockURLSession)
        func testBasicSpeechToText() async throws {
            let mockResponse = """
                {
                    "text": "Hello, this is a test transcription.",
                    "metadata": {
                        "model": "openai/whisper-large-v3",
                        "language": "en",
                        "duration": 3.5
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.speechToText(
                model: "openai/whisper-large-v3",
                audio: "base64_audio_data"
            )

            #expect(result.text == "Hello, this is a test transcription.")
            #expect(result.metadata?["model"] == .string("openai/whisper-large-v3"))
            #expect(result.metadata?["language"] == .string("en"))
        }

        @Test("Speech to text with all parameters", .mockURLSession)
        func testSpeechToTextWithAllParameters() async throws {
            let mockResponse = """
                {
                    "text": "Bonjour, comment allez-vous?",
                    "metadata": {
                        "model": "openai/whisper-large-v3",
                        "language": "fr",
                        "task": "transcribe",
                        "duration": 4.2,
                        "chunk_length": 30,
                        "stride_length": 5
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.speechToText(
                model: "openai/whisper-large-v3",
                audio: "base64_audio_data",
                provider: .custom(name: "openai"),
                language: "fr",
                task: .transcribe,
                returnTimestamps: true,
                chunkLength: 30,
                strideLength: 5
            )

            #expect(result.text == "Bonjour, comment allez-vous?")
            #expect(result.metadata?["language"] == .string("fr"))
            #expect(result.metadata?["task"] == .string("transcribe"))
        }

        @Test("Speech to text with translation task", .mockURLSession)
        func testSpeechToTextWithTranslation() async throws {
            let mockResponse = """
                {
                    "text": "Hello, how are you?",
                    "metadata": {
                        "model": "openai/whisper-large-v3",
                        "language": "en",
                        "task": "translate",
                        "original_language": "fr"
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.speechToText(
                model: "openai/whisper-large-v3",
                audio: "base64_audio_data",
                task: .translate
            )

            #expect(result.text == "Hello, how are you?")
            #expect(result.metadata?["task"] == .string("translate"))
        }

        @Test("Speech to text handles error response", .mockURLSession)
        func testSpeechToTextHandlesError() async throws {
            let errorResponse = """
                {
                    "error": "Invalid audio format"
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 400,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.speechToText(
                    model: "openai/whisper-large-v3",
                    audio: "invalid_audio_data"
                )
            }
        }
    }

#endif  // swift(>=6.1)
