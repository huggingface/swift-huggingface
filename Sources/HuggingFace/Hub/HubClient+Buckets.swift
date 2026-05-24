#if HUGGINGFACE_ENABLE_XET

    import Foundation
    import Xet

    // MARK: - Buckets API

    extension HubClient {

        /// Creates a new bucket on the Hub.
        ///
        /// - Parameters:
        ///   - name: The name of the bucket.
        ///   - organization: The organization to create the repository under (optional).
        ///   - visibility: Public or private. Defaults to public.
        ///   - resourceGroupId: Resource group ID (Enterprise Hub only).
        ///   - region: Cloud region (Team plan or above).
        ///   - existOk: If `true`, return success when the bucket already exists.
        /// - Returns: A tuple containing the URL and identifier of the created bucket.
        /// - Throws: An error if the request fails.
        public func createBucket(
            name: String,
            organization: String? = nil,
            visibility: Repo.Visibility = .public,
            resourceGroupId: String? = nil,
            region: Bucket.Region? = nil,
            existOk: Bool = false
        ) async throws -> (url: String, bucketId: String?) {
            var params: [String: Value] = [:]
            params["private"] = .bool(visibility.isPrivate)
            if let resourceGroupId {
                params["resourceGroupId"] = .string(resourceGroupId)
            }
            if let region {
                params["region"] = .string(region.rawValue)
            }

            // "me" == "the authenticated user", accepted at creation time
            let pathNamespace = organization ?? "me"
            let path = "/api/buckets/\(pathNamespace)/\(name)"

            do {
                let response: CreateBucketResponse = try await httpClient.fetch(.post, path, params: params)
                return (url: response.url, bucketId: Self.parseCanonicalBucketID(fromURL: response.url))
            } catch let error as HTTPClientError
                where existOk && error.code == .responseError && error.statusCode == 409
            {
                var url = httpClient.host.appending(path: "buckets")
                if let organization {
                    url = url.appending(path: organization)
                }
                url = url.appending(path: name)
                // We know the canonical id only when the caller passed an explicit organization
                let bucketId: String? = organization.map { "\($0)/\(name)" }
                return (url: url.absoluteString, bucketId: bucketId)
            }
        }

        /// Extract the canonical `"namespace/name"` from a bucket URL such as
        /// `https://huggingface.co/buckets/{namespace}/{name}`. Returns `nil` if
        /// the URL doesn't match the expected pattern.
        private static func parseCanonicalBucketID(fromURL urlString: String) -> String? {
            guard let url = URL(string: urlString) else { return nil }
            let segments = url.pathComponents.filter { $0 != "/" }
            guard segments.count >= 3, segments.first == "buckets" else { return nil }
            return "\(segments[1])/\(segments[2])"
        }

        /// Gets information about a specific bucket.
        ///
        /// - Parameter id: The bucket identifier (`namespace/name`).
        /// - Returns: The bucket's info.
        /// - Throws: An error if the request fails or the bucket is not found.
        public func getBucket(_ id: Bucket.ID) async throws -> Bucket {
            try await httpClient.fetch(.get, "/api/buckets/\(id.rawValue)")
        }

        /// Lists buckets under a given namespace.
        ///
        /// - Parameters:
        ///   - namespace: Namespace to list under. Defaults to `"me"`, which
        ///     means the authenticated user.
        ///   - search: Optional substring filter on bucket names.
        /// - Returns: A paginated response of bucket info objects.
        /// - Throws: An error if the request fails.
        public func listBuckets(
            namespace: String = "me",
            search: String? = nil
        ) async throws -> PaginatedResponse<Bucket> {
            var params: [String: Value] = [:]
            if let search { params["search"] = .string(search) }

            return try await httpClient.fetchPaginated(.get, "/api/buckets/\(namespace)", params: params)
        }

        /// Deletes a bucket.
        ///
        /// - Parameters:
        ///   - id: The bucket identifier (`namespace/name`).
        ///   - missingOk: If `true`, swallow 404 responses instead of throwing.
        /// - Returns: `true` if the bucket was deleted (or did not exist when `missingOk` is set).
        /// - Throws: An error if the request fails.
        @discardableResult
        public func deleteBucket(
            _ id: Bucket.ID,
            missingOk: Bool = false
        ) async throws -> Bool {
            do {
                let _: Bool = try await httpClient.fetch(.delete, "/api/buckets/\(id.rawValue)")
                return true
            } catch let error as HTTPClientError
                where missingOk && error.code == .responseError && error.statusCode == 404
            {
                return true
            }
        }

        /// Moves (renames or transfers) a bucket to a new location.
        ///
        /// Reuses the shared `/api/repos/move` endpoint with `type: "bucket"`.
        ///
        /// - Parameters:
        ///   - from: The current bucket identifier.
        ///   - to: The new bucket identifier.
        /// - Returns: `true` if the bucket was successfully moved.
        public func moveBucket(from: Bucket.ID, to: Bucket.ID) async throws -> Bool {
            let params: [String: Value] = [
                "fromRepo": .string(from.rawValue),
                "toRepo": .string(to.rawValue),
                "type": .string("bucket"),
            ]
            return try await httpClient.fetch(.post, "/api/repos/move", params: params)
        }

        /// Lists files and folders in a bucket.
        ///
        /// Non-recursive listing yields a mixed sequence of files and folders;
        /// recursive listing yields files only.
        ///
        /// - Parameters:
        ///   - id: The bucket identifier (`namespace/name`).
        ///   - prefix: Filter to entries whose path starts with this prefix.
        ///   - recursive: If `true`, list files recursively.
        /// - Returns: A paginated response of tree entries.
        public func listBucketTree(
            _ id: Bucket.ID,
            prefix: String? = nil,
            recursive: Bool? = nil
        ) async throws -> PaginatedResponse<Bucket.TreeEntry> {
            // Path-encode the prefix; matches Python's `quote(prefix, safe="")`.
            let encodedSuffix: String
            if let prefix, !prefix.isEmpty {
                let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_.~"))
                let encoded = prefix.addingPercentEncoding(withAllowedCharacters: allowed) ?? prefix
                encodedSuffix = "/" + encoded
            } else {
                encodedSuffix = ""
            }

            var params: [String: Value] = [:]
            if let recursive { params["recursive"] = .bool(recursive) }

            return try await httpClient.fetchPaginated(
                .get,
                "/api/buckets/\(id.rawValue)/tree\(encodedSuffix)",
                params: params
            )
        }

        /// Gets metadata for a batch of files in a bucket.
        ///
        /// Paths that do not exist are silently omitted from the response.
        ///
        /// - Parameters:
        ///   - id: The bucket identifier (`namespace/name`).
        ///   - paths: File paths to look up. Should be ≤ 1000 entries per call;
        ///     callers should batch larger lists themselves.
        /// - Returns: An array of `Bucket.File` entries for the paths that exist.
        public func getBucketFiles(
            _ id: Bucket.ID,
            paths: [String]
        ) async throws -> [Bucket.File] {
            let params: [String: Value] = ["paths": .array(paths.map(Value.string))]
            return try await httpClient.fetch(
                .post,
                "/api/buckets/\(id.rawValue)/paths-info",
                params: params
            )
        }

        // MARK: - Download

        /// Downloads a single file from a bucket to a local destination.
        ///
        /// Issues a HEAD against the resolve endpoint to retrieve the file's
        /// Xet hash, then streams the content via the Xet downloader. If you
        /// already have a `Bucket.File` from `listBucketTree` /
        /// `getBucketPathsInfo`, the variant that takes one directly skips the
        /// HEAD round-trip.
        ///
        /// - Parameters:
        ///   - remotePath: Path of the file within the bucket.
        ///   - bucketID: The bucket identifier (`namespace/name`).
        ///   - destination: Local file URL to write the downloaded content to.
        ///   - progress: Optional progress object. Currently reports completion
        ///     as a single 0 → 100 step at the end of the download — fine-
        ///     grained progress is a follow-up tied to swift-xet's API.
        /// - Returns: The destination URL.
        /// - Throws: An error if the HEAD fails, the response lacks Xet
        ///   metadata, or the Xet download fails.
        @discardableResult
        public func downloadBucketFile(
            at remotePath: String,
            in bucketID: Bucket.ID,
            to destination: URL,
            progress: Progress? = nil
        ) async throws -> URL {
            let resolveURL = httpClient.host
                .appending(path: "buckets")
                .appending(path: bucketID.namespace)
                .appending(path: bucketID.name)
                .appending(path: "resolve")
                .appending(path: remotePath)

            var request = try await httpClient.createRequest(.head, url: resolveURL)
            request.cachePolicy = .reloadIgnoringLocalCacheData

            let response: URLResponse
            #if canImport(FoundationNetworking)
                (_, response) = try await metadataSession.data(for: request)
            #else
                (_, response) = try await session.data(
                    for: request,
                    delegate: SameHostRedirectDelegate.shared
                )
            #endif

            guard let metadata = XetFileMetadata(response: response) else {
                throw HTTPClientError.requestError(
                    "Bucket file '\(remotePath)' did not return Xet metadata; bucket downloads require Xet."
                )
            }

            return try await downloadBucketContent(
                xetHash: metadata.fileID,
                in: bucketID,
                to: destination,
                progress: progress
            )
        }

        /// Downloads a pre-resolved bucket file (returned by `listBucketTree`
        /// or `getBucketPathsInfo`) directly via Xet, skipping the metadata HEAD.
        ///
        /// - Parameters:
        ///   - file: The bucket file to download.
        ///   - bucketID: The bucket identifier the file lives in.
        ///   - destination: Local file URL to write the downloaded content to.
        ///   - progress: Optional progress object (see ``downloadBucketFile(at:in:to:progress:)``).
        /// - Returns: The destination URL.
        @discardableResult
        public func downloadBucketFile(
            _ file: Bucket.File,
            in bucketID: Bucket.ID,
            to destination: URL,
            progress: Progress? = nil
        ) async throws -> URL {
            try await downloadBucketContent(
                xetHash: file.xetHash,
                in: bucketID,
                to: destination,
                progress: progress
            )
        }

        /// Shared Xet download body for the two public overloads.
        private func downloadBucketContent(
            xetHash: String,
            in bucketID: Bucket.ID,
            to destination: URL,
            progress: Progress?
        ) async throws -> URL {
            // Mirrors `xetRefreshURL(for:kind:revision:)` in `HubClient+Files.swift`
            // for repos. Buckets follow the same `xet-read-token` pattern with no
            // revision segment.
            let refreshURL = httpClient.host
                .appending(path: "api")
                .appending(path: "buckets")
                .appending(path: bucketID.namespace)
                .appending(path: bucketID.name)
                .appending(path: "xet-read-token")

            _ = try await Xet.withDownloader(
                refreshURL: refreshURL,
                hubToken: try? await httpClient.tokenProvider.getToken()
            ) { downloader in
                try await downloader.download(xetHash, to: destination)
            }

            // Mirrors `downloadFileWithXet` — swift-xet doesn't expose per-byte
            // progress today, so we surface a single completion step.
            progress?.totalUnitCount = 100
            progress?.completedUnitCount = 100

            return destination
        }
    }

    // MARK: - Private response types

    private struct CreateBucketResponse: Decodable, Sendable {
        let url: String
    }

#endif  // HUGGINGFACE_ENABLE_XET
