import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)

    /// Tests for the Text to Image API
    @Suite("Text to Image Tests", .serialized)
    struct TextToImageTests {
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
            "Basic text to image generation",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/images/generations",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testBasicTextToImage() async throws {
            let client = createClient()
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

        @Test(
            "Text to image with all parameters",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/images/generations",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testTextToImageWithAllParameters() async throws {
            let client = createClient()
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

        @Test(
            "Text to image with LoRA configuration",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/images/generations",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testTextToImageWithLoRA() async throws {
            let client = createClient()
            let loras = [
                TextToImage.Lora(name: "anime-style", strength: 0.8)
            ]

            let result = try await client.textToImage(
                model: "stabilityai/stable-diffusion-xl-base-1.0",
                prompt: "A stunning anime character",
                loras: loras
            )

            let expectedImageData = Data(
                base64Encoded:
                    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
            )!
            #expect(result.image == expectedImageData)
        }

        @Test(
            "Text to image with ControlNet configuration",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/images/generations",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testTextToImageWithControlNet() async throws {
            let client = createClient()
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

        @Test(
            "Text to image with different aspect ratios",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/images/generations",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testTextToImageWithDifferentAspectRatios() async throws {
            let client = createClient()
            let result = try await client.textToImage(
                model: "stabilityai/stable-diffusion-xl-base-1.0",
                prompt: "A wide landscape view",
                width: 1920,
                height: 1080
            )

            #expect(result.metadata?["width"] == .int(1920))
            #expect(result.metadata?["height"] == .int(1080))
        }

        @Test(
            "Text to image handles error response",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/images/generations",
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
        func testTextToImageHandlesError() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.textToImage(
                    model: "unavailable-model",
                    prompt: "Test prompt"
                )
            }
        }

        @Test(
            "Text to image handles invalid prompt",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/images/generations",
                        400,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Prompt contains inappropriate content"
                        }
                        """
                    }
                ]
            )
        )
        func testTextToImageHandlesInvalidPrompt() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.textToImage(
                    model: "stabilityai/stable-diffusion-xl-base-1.0",
                    prompt: "inappropriate content"
                )
            }
        }

        @Test(
            "Text to image with high resolution",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/images/generations",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testTextToImageWithHighResolution() async throws {
            let client = createClient()
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
