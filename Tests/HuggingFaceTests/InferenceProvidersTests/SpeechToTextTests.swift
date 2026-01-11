import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)

    /// Tests for the Speech to Text API
    @Suite("Speech to Text Tests", .serialized)
    struct SpeechToTextTests {
        private func createClient() -> InferenceClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
            let session = URLSession(configuration: configuration)

            return InferenceClient(
                session: session,
                host: URL(string: "https://router.huggingface.co")!,
                userAgent: "TestClient/1.0"
            )
        }

        @Test(
            "Basic speech to text transcription",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/audio/transcriptions",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "text": "Hello, this is a test transcription.",
                            "metadata": {
                                "model": "openai/whisper-large-v3",
                                "language": "en",
                                "duration": 3.5
                            }
                        }
                        """
                    }
                ]
            )
        )
        func testBasicSpeechToText() async throws {
            let client = createClient()
            let result = try await client.speechToText(
                model: "openai/whisper-large-v3",
                audio: "base64_audio_data"
            )

            #expect(result.text == "Hello, this is a test transcription.")
            #expect(result.metadata?["model"] == .string("openai/whisper-large-v3"))
            #expect(result.metadata?["language"] == .string("en"))
        }

        @Test(
            "Speech to text with all parameters",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/audio/transcriptions",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testSpeechToTextWithAllParameters() async throws {
            let client = createClient()
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

        @Test(
            "Speech to text with translation task",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/audio/transcriptions",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testSpeechToTextWithTranslation() async throws {
            let client = createClient()
            let result = try await client.speechToText(
                model: "openai/whisper-large-v3",
                audio: "base64_audio_data",
                task: .translate
            )

            #expect(result.text == "Hello, how are you?")
            #expect(result.metadata?["task"] == .string("translate"))
        }

        @Test(
            "Speech to text handles error response",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/audio/transcriptions",
                        400,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Invalid audio format"
                        }
                        """
                    }
                ]
            )
        )
        func testSpeechToTextHandlesError() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.speechToText(
                    model: "openai/whisper-large-v3",
                    audio: "invalid_audio_data"
                )
            }
        }
    }

#endif  // swift(>=6.1)
