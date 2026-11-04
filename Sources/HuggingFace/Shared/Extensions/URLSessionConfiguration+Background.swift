import Foundation

#if !canImport(FoundationNetworking)
    extension URLSessionConfiguration {
        /// Whether this is a background configuration,
        /// created with `URLSessionConfiguration.background(withIdentifier:)`.
        var isBackground: Bool {
            identifier != nil
        }

        /// Returns a default configuration with the settings of this configuration.
        ///
        /// The returned configuration has no identifier,
        /// so a session created from it runs requests in the app's process.
        /// Settings that apply only to background sessions,
        /// such as `isDiscretionary`, `sessionSendsLaunchEvents`,
        /// and `sharedContainerIdentifier`, are not copied.
        func foregroundCopy() -> URLSessionConfiguration {
            let copy = URLSessionConfiguration.default

            copy.requestCachePolicy = requestCachePolicy
            copy.timeoutIntervalForRequest = timeoutIntervalForRequest
            copy.timeoutIntervalForResource = timeoutIntervalForResource
            copy.networkServiceType = networkServiceType
            copy.allowsCellularAccess = allowsCellularAccess
            copy.allowsExpensiveNetworkAccess = allowsExpensiveNetworkAccess
            copy.allowsConstrainedNetworkAccess = allowsConstrainedNetworkAccess
            copy.requiresDNSSECValidation = requiresDNSSECValidation
            copy.waitsForConnectivity = waitsForConnectivity
            copy.connectionProxyDictionary = connectionProxyDictionary
            copy.tlsMinimumSupportedProtocolVersion = tlsMinimumSupportedProtocolVersion
            copy.tlsMaximumSupportedProtocolVersion = tlsMaximumSupportedProtocolVersion
            copy.httpShouldSetCookies = httpShouldSetCookies
            copy.httpCookieAcceptPolicy = httpCookieAcceptPolicy
            copy.httpAdditionalHeaders = httpAdditionalHeaders
            copy.httpMaximumConnectionsPerHost = httpMaximumConnectionsPerHost
            copy.httpCookieStorage = httpCookieStorage
            copy.urlCredentialStorage = urlCredentialStorage
            copy.urlCache = urlCache
            copy.protocolClasses = protocolClasses

            #if os(iOS) && !targetEnvironment(macCatalyst)
                copy.multipathServiceType = multipathServiceType
            #endif

            #if compiler(>=6.1)
                if #available(macOS 15.4, iOS 18.4, watchOS 11.4, tvOS 18.4, visionOS 2.4, *) {
                    copy.usesClassicLoadingMode = usesClassicLoadingMode
                }
            #endif

            #if compiler(>=6.2)
                if #available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *) {
                    copy.enablesEarlyData = enablesEarlyData
                }
            #endif

            return copy
        }
    }
#endif
