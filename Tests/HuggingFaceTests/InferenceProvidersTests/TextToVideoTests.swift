import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

#if swift(>=6.1)

    /// Tests for the Text to Video API
    @Suite("Text to Video Tests", .serialized)
    struct TextToVideoTests {
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

        @Test("Basic text to video generation", .mockURLSession)
        func testBasicTextToVideo() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/v1/videos/generations")
                #expect(request.httpMethod == "POST")

                // Verify request body contains expected parameters
                if let body = request.httpBody {
                    let json = try JSONSerialization.jsonObject(with: body) as! [String: Any]
                    #expect(json["model"] as? String == "zeroscope_v2_576w")
                    #expect(json["prompt"] as? String == "A cat playing with a ball")
                }

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
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

        @Test("Text to video with all parameters", .mockURLSession)
        func testTextToVideoWithAllParameters() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                // Verify request body contains expected parameters
                if let body = request.httpBody {
                    let json = try JSONSerialization.jsonObject(with: body) as! [String: Any]
                    #expect(json["model"] as? String == "zeroscope_v2_576w")
                    #expect(json["prompt"] as? String == "A dancing robot")
                }
                #expect(request.url?.path == "/v1/videos/generations")
                #expect(request.httpMethod == "POST")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
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

        @Test("Text to video handles error response", .mockURLSession)
        func testTextToVideoHandlesError() async throws {
            let errorResponse = """
                {
                    "error": "Model not available"
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 503,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.textToVideo(
                    model: "unavailable-model",
                    prompt: "Test prompt"
                )
            }
        }
    }

#endif  // swift(>=6.1)
