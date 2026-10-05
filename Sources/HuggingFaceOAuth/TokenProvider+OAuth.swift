import Foundation
import HuggingFace

#if canImport(AuthenticationServices)
    extension TokenProvider {
        /// Creates an OAuth token provider using HuggingFaceAuthenticationManager.
        ///
        /// Use this factory method for OAuth-based authentication flows. The authentication
        /// manager handles the complete OAuth flow including token refresh.
        ///
        /// ```swift
        /// let authManager = try HuggingFaceAuthenticationManager(
        ///     clientID: "your-client-id",
        ///     redirectURL: URL(string: "myapp://oauth")!,
        ///     scope: .basic,
        ///     keychainService: "com.example.app",
        ///     keychainAccount: "huggingface"
        /// )
        /// let client = HubClient(tokenProvider: .oauth(manager: authManager))
        /// ```
        ///
        /// - Parameter manager: The OAuth authentication manager that handles token retrieval and refresh.
        /// - Returns: A token provider that retrieves tokens from the authentication manager.
        @available(macOS 14.0, macCatalyst 17.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
        public static func oauth(manager: HuggingFaceAuthenticationManager) -> TokenProvider {
            return .oauth(getToken: { @MainActor in
                try await manager.getValidToken()
            })
        }
    }
#endif
