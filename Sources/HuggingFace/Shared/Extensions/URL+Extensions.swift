import Foundation

#if os(Linux)
    extension URL {
        /// A compatibility shim for `URL.cachesDirectory` on Linux.
        ///
        /// This implementation relies on `FileManager` to determine the cache directory,
        /// falling back to standard Linux locations if necessary.
        ///
        /// - Warning: This property implementation uses `force-unwrap` which matches behavior
        ///   on Apple platforms where this property is available and assumed safe.
        static var cachesDirectory: URL {
            FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first!
        }
    }
#endif
