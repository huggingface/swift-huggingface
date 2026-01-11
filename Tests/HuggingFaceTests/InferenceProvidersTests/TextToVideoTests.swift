import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)

    /// Tests for the Text to Video API
    @Suite("Text to Video Tests", .serialized)
    struct TextToVideoTests {
        /// Helper to create a URL session with Replay protocol handlers
        func createClient() -> InferenceClient {
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
            "Basic text to video generation",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/videos/generations",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "video": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
                            "mime_type": "video/mp4",
                            "metadata": {
                                "model": "zeroscope_v2_576w",
                                "width": 576,
                                "height": 320,
                                "num_frames": 24,
                                "frame_rate": 8
                            }
                        }
                        """
                    }
                ]
            )
        )
        func testBasicTextToVideo() async throws {
            let client = createClient()
            let result = try await client.textToVideo(
                model: "zeroscope_v2_576w",
                prompt: "A cat playing with a ball"
            )

            let expectedVideoData = Data(
                base64Encoded:
                    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
            )!
            #expect(result.video == expectedVideoData)
            #expect(result.mimeType == "video/mp4")
            #expect(result.metadata?["width"] == .int(576))
            #expect(result.metadata?["height"] == .int(320))
        }

        @Test(
            "Text to video with all parameters",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/videos/generations",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "video": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
                            "mime_type": "video/mp4",
                            "metadata": {
                                "model": "zeroscope_v2_576w",
                                "width": 1024,
                                "height": 576,
                                "num_frames": 48,
                                "frame_rate": 24,
                                "num_videos": 2,
                                "guidance_scale": 7.5,
                                "num_inference_steps": 50,
                                "seed": 123,
                                "duration": 2.0,
                                "motion_strength": 0.8
                            }
                        }
                        """
                    }
                ]
            )
        )
        func testTextToVideoWithAllParameters() async throws {
            let client = createClient()
            let result = try await client.textToVideo(
                model: "zeroscope_v2_576w",
                prompt: "A dancing robot",
                provider: .hfInference,
                negativePrompt: "blurry, low quality",
                width: 1024,
                height: 576,
                numFrames: 48,
                frameRate: 24,
                numVideos: 2,
                guidanceScale: 7.5,
                numInferenceSteps: 50,
                seed: 123,
                safetyChecker: true,
                enhancePrompt: false,
                duration: 2.0,
                motionStrength: 0.8
            )

            let expectedVideoData = Data(
                base64Encoded:
                    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
            )!
            #expect(result.video == expectedVideoData)
            #expect(result.metadata?["num_videos"] == .int(2))
            #expect(result.metadata?["duration"] == .int(2))
        }

        @Test(
            "Text to video handles error response",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/videos/generations",
                        503,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Model not available"
                        }
                        """
                    }
                ]
            )
        )
        func testTextToVideoHandlesError() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.textToVideo(
                    model: "unavailable-model",
                    prompt: "Test prompt"
                )
            }
        }
    }

#endif  // swift(>=6.1)
