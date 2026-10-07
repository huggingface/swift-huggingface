import Foundation

/// A provider for Hugging Face Inference Providers.
///
/// Use one of the built-in providers, such as ``groq``,
/// or ``custom(name:baseURL:)`` for one this package doesn't list yet.
/// New built-in providers may be added in minor releases.
///
/// - SeeAlso: [Inference Providers Documentation](https://huggingface.co/docs/inference-providers/index)
public struct Provider: Hashable, Sendable {
    /// The identifier used in API requests for this provider.
    public let identifier: String

    /// The display name for this provider.
    public let displayName: String

    /// The capabilities supported by this provider.
    public let capabilities: Set<Capability>

    /// The base URL of a custom provider, if it has one.
    public let baseURL: URL?

    /// Whether this is a custom provider rather than a built-in one.
    let isCustom: Bool

    private init(
        identifier: String,
        displayName: String,
        capabilities: Set<Capability>,
        baseURL: URL? = nil,
        isCustom: Bool = false
    ) {
        self.identifier = identifier
        self.displayName = displayName
        self.capabilities = capabilities
        self.baseURL = baseURL
        self.isCustom = isCustom
    }

    /// Automatically select the best available provider for the model.
    public static let auto = Provider(
        identifier: "auto",
        displayName: "Auto",
        capabilities: Set(Capability.allCases)
    )

    // MARK: - Built-in Providers

    /// Cerebras provider for high-performance inference.
    public static let cerebras = Provider(
        identifier: "cerebras",
        displayName: "Cerebras",
        capabilities: [.chatCompletion]
    )

    /// Cohere provider for language models and vision-language models.
    public static let cohere = Provider(
        identifier: "cohere",
        displayName: "Cohere",
        capabilities: [.chatCompletion, .chatCompletionVLM]
    )

    /// Fal AI provider for various AI tasks.
    public static let falAI = Provider(
        identifier: "fal-ai",
        displayName: "Fal AI",
        capabilities: [.chatCompletion, .chatCompletionVLM, .featureExtraction]
    )

    /// Featherless AI provider for fast inference.
    public static let featherlessAI = Provider(
        identifier: "featherless-ai",
        displayName: "Featherless AI",
        capabilities: [.chatCompletion, .chatCompletionVLM]
    )

    /// Fireworks AI provider for language and vision-language models.
    public static let fireworks = Provider(
        identifier: "fireworks-ai",
        displayName: "Fireworks AI",
        capabilities: [.chatCompletion, .chatCompletionVLM]
    )

    /// Groq provider for ultra-fast inference.
    public static let groq = Provider(
        identifier: "groq",
        displayName: "Groq",
        capabilities: [.chatCompletion, .chatCompletionVLM]
    )

    /// Hugging Face Inference provider for comprehensive model support.
    public static let hfInference = Provider(
        identifier: "hf-inference",
        displayName: "Hugging Face Inference",
        capabilities: [.chatCompletion, .chatCompletionVLM, .featureExtraction, .textToImage, .textToVideo]
    )

    /// Hyperbolic provider for specialized inference.
    public static let hyperbolic = Provider(
        identifier: "hyperbolic",
        displayName: "Hyperbolic",
        capabilities: [.chatCompletion, .chatCompletionVLM]
    )

    /// Nebius provider for cloud-based inference.
    public static let nebius = Provider(
        identifier: "nebius",
        displayName: "Nebius",
        capabilities: [.chatCompletion, .chatCompletionVLM, .featureExtraction, .textToImage]
    )

    /// Novita provider for various AI tasks.
    public static let novita = Provider(
        identifier: "novita",
        displayName: "Novita",
        capabilities: [.chatCompletion, .chatCompletionVLM, .featureExtraction]
    )

    /// Nscale provider for scalable inference.
    public static let nscale = Provider(
        identifier: "nscale",
        displayName: "Nscale",
        capabilities: [.chatCompletion, .chatCompletionVLM, .featureExtraction]
    )

    /// Public AI provider for open models.
    public static let publicAI = Provider(
        identifier: "public-ai",
        displayName: "Public AI",
        capabilities: [.chatCompletion]
    )

    /// Replicate provider for model hosting and inference.
    public static let replicate = Provider(
        identifier: "replicate",
        displayName: "Replicate",
        capabilities: [.chatCompletion, .chatCompletionVLM, .featureExtraction]
    )

    /// SambaNova provider for enterprise-grade inference.
    public static let sambaNova = Provider(
        identifier: "sambanova",
        displayName: "SambaNova",
        capabilities: [.chatCompletion, .chatCompletionVLM]
    )

    /// Scaleway provider for European cloud inference.
    public static let scaleway = Provider(
        identifier: "scaleway",
        displayName: "Scaleway",
        capabilities: [.chatCompletion, .chatCompletionVLM]
    )

    /// Together AI provider for various AI tasks.
    public static let together = Provider(
        identifier: "together",
        displayName: "Together AI",
        capabilities: [.chatCompletion, .chatCompletionVLM, .featureExtraction]
    )

    /// Z.ai provider for specialized inference.
    public static let zai = Provider(
        identifier: "zai-org",
        displayName: "Z.ai",
        capabilities: [.chatCompletion, .chatCompletionVLM]
    )

    // MARK: - Custom Provider

    /// A custom provider with a specific name and optional base URL.
    ///
    /// A custom provider is assumed to support all capabilities.
    ///
    /// - Parameters:
    ///   - name: The name of the custom provider.
    ///   - baseURL: An optional custom base URL for the provider.
    public static func custom(name: String, baseURL: URL? = nil) -> Provider {
        Provider(
            identifier: name,
            displayName: name,
            capabilities: Set(Capability.allCases),
            baseURL: baseURL,
            isCustom: true
        )
    }

    /// The built-in providers, including ``auto``.
    static let builtIn: [Provider] = [
        .auto,
        .cerebras,
        .cohere,
        .falAI,
        .featherlessAI,
        .fireworks,
        .groq,
        .hfInference,
        .hyperbolic,
        .nebius,
        .novita,
        .nscale,
        .publicAI,
        .replicate,
        .sambaNova,
        .scaleway,
        .together,
        .zai,
    ]
}

// MARK: - Capability

/// Represents the capabilities supported by inference providers.
public enum Capability: String, Hashable, CaseIterable, Codable, Sendable {
    /// Chat completion with language models.
    case chatCompletion = "chat_completion"

    /// Chat completion with vision-language models.
    case chatCompletionVLM = "chat_completion_vlm"

    /// Feature extraction and embeddings.
    case featureExtraction = "feature_extraction"

    /// Text-to-image generation.
    case textToImage = "text_to_image"

    /// Text-to-video generation.
    case textToVideo = "text_to_video"

    /// Speech-to-text transcription.
    case speechToText = "speech_to_text"

    /// Text-to-speech synthesis.
    case textToSpeech = "text_to_speech"

    /// Image-to-text generation.
    case imageToText = "image_to_text"

    /// Image classification.
    case imageClassification = "image_classification"

    /// Text classification.
    case textClassification = "text_classification"

    /// Summarization.
    case summarization = "summarization"

    /// Translation.
    case translation = "translation"

    /// Question answering.
    case questionAnswering = "question_answering"

    /// Zero-shot classification.
    case zeroShotClassification = "zero_shot_classification"

    /// Conversational AI.
    case conversational = "conversational"

    /// Fill mask tasks.
    case fillMask = "fill_mask"

    /// Token classification (NER).
    case tokenClassification = "token_classification"

    /// Table question answering.
    case tableQuestionAnswering = "table_question_answering"

    /// Text generation.
    case textGeneration = "text_generation"

    /// Multiple choice.
    case multipleChoice = "multiple_choice"

    /// Sentence similarity.
    case sentenceSimilarity = "sentence_similarity"

    /// Text-to-audio generation.
    case textToAudio = "text_to_audio"
}

// MARK: - Codable

extension Provider: Codable {
    public init(from decoder: Decoder) throws {
        // Try to decode as a string first (for built-in providers)
        if let container = try? decoder.singleValueContainer(),
            let identifier = try? container.decode(String.self)
        {
            self = Self.builtIn.first { $0.identifier == identifier } ?? .custom(name: identifier)
            return
        }

        // Try to decode as a dictionary (for custom providers with baseURL)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let name = try container.decode(String.self, forKey: .name)
        let baseURL = try container.decodeIfPresent(URL.self, forKey: .baseURL)
        self = .custom(name: name, baseURL: baseURL)
    }

    public func encode(to encoder: Encoder) throws {
        if isCustom {
            // For custom providers, encode as a dictionary to preserve baseURL
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(identifier, forKey: .name)
            try container.encodeIfPresent(baseURL, forKey: .baseURL)
        } else {
            // For built-in providers, encode as a simple string
            var container = encoder.singleValueContainer()
            try container.encode(identifier)
        }
    }

    private enum CodingKeys: String, CodingKey {
        case name
        case baseURL
    }
}
