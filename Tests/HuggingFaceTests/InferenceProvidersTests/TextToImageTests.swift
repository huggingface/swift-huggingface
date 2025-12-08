import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)

    /// Tests for the Text to Image API
    @Suite("Text to Image Tests", .serialized)
    struct TextToImageTests {
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

        @Test("Basic text to image generation", .mockURLSession)
        func testBasicTextToImage() async throws {
            let mockResponse = """
                {
                    "image": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
                    "mime_type": "image/png",
                    "metadata": {
                        "model": "stabilityai/stable-diffusion-xl-base-1.0",
                        "width": 1024,
                        "height": 1024,
                        "steps": 20,
                        "guidance_scale": 7.5
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/v1/images/generations")
                #expect(request.httpMethod == "POST")

                // Verify request body contains expected parameters
                if let body = request.httpBody {
                    let json = try JSONSerialization.jsonObject(with: body) as! [String: Any]
                    #expect(json["model"] as? String == "stabilityai/stable-diffusion-xl-base-1.0")
                    #expect(json["prompt"] as? String == "A beautiful sunset over mountains")
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
            let result = try await client.textToImage(
                model: "stabilityai/stable-diffusion-xl-base-1.0",
                prompt: "A beautiful sunset over mountains"
            )

            let expectedImageData = Data(
                base64Encoded:
                    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
            )!
            #expect(result.image == expectedImageData)
            #expect(result.mimeType == "image/png")
            #expect(result.metadata?["model"] == .string("stabilityai/stable-diffusion-xl-base-1.0"))
            #expect(result.metadata?["width"] == .int(1024))
            #expect(result.metadata?["height"] == .int(1024))
        }

        @Test("Text to image with all parameters", .mockURLSession)
        func testTextToImageWithAllParameters() async throws {
            let mockResponse = """
                {
                    "image": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
                    "mime_type": "image/jpeg",
                    "metadata": {
                        "model": "stabilityai/stable-diffusion-xl-base-1.0",
                        "width": 512,
                        "height": 768,
                        "num_images": 2,
                        "guidance_scale": 8.0,
                        "num_inference_steps": 30,
                        "seed": 42
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                // Verify request body contains expected parameters
                if let body = request.httpBody {
                    let json = try JSONSerialization.jsonObject(with: body) as! [String: Any]
                    #expect(json["model"] as? String == "stabilityai/stable-diffusion-xl-base-1.0")
                    #expect(json["prompt"] as? String == "A beautiful sunset over mountains")
                }

                #expect(request.url?.path == "/v1/images/generations")
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
            let result = try await client.textToImage(
                model: "stabilityai/stable-diffusion-xl-base-1.0",
                prompt: "A futuristic city",
                provider: .custom(name: "stabilityai"),
                negativePrompt: "blurry, low quality",
                width: 512,
                height: 768,
                numImages: 2,
                guidanceScale: 8.0,
                numInferenceSteps: 30,
                seed: 42,
                safetyChecker: true,
                enhancePrompt: false,
                multiLingual: true,
                panorama: false,
                selfAttention: true,
                upscale: false,
                embeddingsModel: "clip"
            )

            let expectedImageData = Data(
                base64Encoded:
                    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
            )!
            #expect(result.image == expectedImageData)
            #expect(result.mimeType == "image/jpeg")
            #expect(result.metadata?["num_images"] == .int(2))
            #expect(result.metadata?["seed"] == .int(42))
        }

        @Test("Text to image with LoRA configuration", .mockURLSession)
        func testTextToImageWithLoRA() async throws {
            let mockResponse = """
                {
                    "image": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
                    "mime_type": "image/png",
                    "metadata": {
                        "model": "stabilityai/stable-diffusion-xl-base-1.0",
                        "loras": [
                            {
                                "name": "anime-style",
                                "strength": 0.8
                            }
                        ]
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                // Verify request body contains expected parameters
                if let body = request.httpBody {
                    let json = try JSONSerialization.jsonObject(with: body) as! [String: Any]
                    #expect(json["model"] as? String == "stabilityai/stable-diffusion-xl-base-1.0")
                    #expect(json["prompt"] as? String == "A beautiful sunset over mountains")
                }

                #expect(request.url?.path == "/v1/images/generations")
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
            let loras = [
                TextToImage.Lora(name: "anime-style", strength: 0.8)
            ]

            let result = try await client.textToImage(
                model: "stabilityai/stable-diffusion-xl-base-1.0",
                prompt: "A beautiful anime character",
                loras: loras
            )

            let expectedImageData = Data(
                base64Encoded:
                    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
            )!
            #expect(result.image == expectedImageData)
        }

        @Test("Text to image with ControlNet configuration", .mockURLSession)
        func testTextToImageWithControlNet() async throws {
            let mockResponse = """
                {
                    "image": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
                    "mime_type": "image/png",
                    "metadata": {
                        "model": "stabilityai/stable-diffusion-xl-base-1.0",
                        "controlnet": {
                            "name": "canny-edge",
                            "strength": 0.9
                        }
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                // Verify request body contains expected parameters
                if let body = request.httpBody {
                    let json = try JSONSerialization.jsonObject(with: body) as! [String: Any]
                    #expect(json["model"] as? String == "stabilityai/stable-diffusion-xl-base-1.0")
                    #expect(json["prompt"] as? String == "A beautiful sunset over mountains")
                }

                #expect(request.url?.path == "/v1/images/generations")
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
            let controlnet = TextToImage.ControlNet(
                name: "canny-edge",
                strength: 0.9,
                controlImage: Data(
                    base64Encoded:
                        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
                )!
            )

            let result = try await client.textToImage(
                model: "stabilityai/stable-diffusion-xl-base-1.0",
                prompt: "A detailed architectural drawing",
                controlnet: controlnet
            )

            let expectedImageData = Data(
                base64Encoded:
                    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
            )!
            #expect(result.image == expectedImageData)
        }

        @Test("Text to image with different aspect ratios", .mockURLSession)
        func testTextToImageWithDifferentAspectRatios() async throws {
            let mockResponse = """
                {
                    "image": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
                    "mime_type": "image/png",
                    "metadata": {
                        "model": "stabilityai/stable-diffusion-xl-base-1.0",
                        "width": 1920,
                        "height": 1080,
                        "aspect_ratio": "16:9"
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                // Verify request body contains expected parameters
                if let body = request.httpBody {
                    let json = try JSONSerialization.jsonObject(with: body) as! [String: Any]
                    #expect(json["model"] as? String == "stabilityai/stable-diffusion-xl-base-1.0")
                    #expect(json["prompt"] as? String == "A beautiful sunset over mountains")
                }
                #expect(request.url?.path == "/v1/images/generations")
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
            let result = try await client.textToImage(
                model: "stabilityai/stable-diffusion-xl-base-1.0",
                prompt: "A wide landscape view",
                width: 1920,
                height: 1080
            )

            #expect(result.metadata?["width"] == .int(1920))
            #expect(result.metadata?["height"] == .int(1080))
        }

        @Test("Text to image handles error response", .mockURLSession)
        func testTextToImageHandlesError() async throws {
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
                _ = try await client.textToImage(
                    model: "unavailable-model",
                    prompt: "Test prompt"
                )
            }
        }

        @Test("Text to image handles invalid prompt", .mockURLSession)
        func testTextToImageHandlesInvalidPrompt() async throws {
            let errorResponse = """
                {
                    "error": "Prompt contains inappropriate content"
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
                _ = try await client.textToImage(
                    model: "stabilityai/stable-diffusion-xl-base-1.0",
                    prompt: "inappropriate content"
                )
            }
        }

        @Test("Text to image with high resolution", .mockURLSession)
        func testTextToImageWithHighResolution() async throws {
            let mockResponse = """
                {
                    "image": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
                    "mime_type": "image/png",
                    "metadata": {
                        "model": "stabilityai/stable-diffusion-xl-base-1.0",
                        "width": 2048,
                        "height": 2048,
                        "upscaled": true
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                // Verify request body contains expected parameters
                if let body = request.httpBody {
                    let json = try JSONSerialization.jsonObject(with: body) as! [String: Any]
                    #expect(json["model"] as? String == "stabilityai/stable-diffusion-xl-base-1.0")
                    #expect(json["prompt"] as? String == "A beautiful sunset over mountains")
                }
                #expect(request.url?.path == "/v1/images/generations")
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
            let result = try await client.textToImage(
                model: "stabilityai/stable-diffusion-xl-base-1.0",
                prompt: "High resolution artwork",
                width: 2048,
                height: 2048,
                upscale: true
            )

            #expect(result.metadata?["upscaled"] == .bool(true))
        }
    }

#endif  // swift(>=6.1)
