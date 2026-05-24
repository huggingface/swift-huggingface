#if HUGGINGFACE_ENABLE_XET

    import Foundation

    #if canImport(FoundationNetworking)
        import FoundationNetworking
    #endif
    import Testing

    @testable import HuggingFace

    /// End-to-end tests that hit the Hub. Gated on `RUN_HUB_E2E_TESTS=1`.
    ///
    /// The write test (`createAndDeleteBucket`) additionally requires `HF_TOKEN`
    /// in the environment.
    @Suite("Bucket Integration Tests")
    struct BucketIntegrationTests {
        private static let runE2E: Bool =
            ProcessInfo.processInfo.environment["RUN_HUB_E2E_TESTS"] == "1"

        private static let hasToken: Bool =
            (ProcessInfo.processInfo.environment["HF_TOKEN"]?.isEmpty == false)

        @Test(
            "Public bucket info and tree listing succeed without a token",
            .enabled(if: runE2E)
        )
        func readPublicBucket() async throws {
            // Explicitly unauthenticated
            let client = HubClient(
                host: URL(string: "https://huggingface.co")!,
                bearerToken: nil
            )

            let id = Bucket.ID(namespace: "huggingface", name: "skills")

            let info = try await client.getBucket(id)
            #expect(info.id.namespace == "huggingface")
            #expect(info.id.name == "skills")
            #expect(info.visibility?.isPublic == true)

            // Non-recursive root listing — should return at least one entry.
            let firstPage = try await client.listBucketTree(id)
            #expect(!firstPage.items.isEmpty)

            // Every file entry must carry a non-empty xetHash (bucket invariant).
            for entry in firstPage.items {
                if case .file(let file) = entry {
                    #expect(!file.xetHash.isEmpty)
                }
            }
        }

        // MARK: - Download from a public bucket (no token)

        @Test(
            "Download a file from a public bucket without a token",
            .enabled(if: runE2E)
        )
        func downloadFromPublicBucket() async throws {
            let client = HubClient(
                host: URL(string: "https://huggingface.co")!,
                bearerToken: nil
            )

            let id = Bucket.ID(namespace: "huggingface", name: "skills")

            // Find any file via the tree listing; pick a small one if there's a
            // choice to keep the test fast.
            let page = try await client.listBucketTree(id, recursive: true)
            let files: [Bucket.File] = page.items.compactMap { entry in
                if case .file(let file) = entry { return file } else { return nil }
            }
            guard let pick = files.min(by: { $0.size < $1.size }) else {
                Issue.record("Public bucket returned no files")
                return
            }

            // Download to a unique temp file and verify size matches the
            // listing's `size` field.
            let tempURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("bucket-e2e-\(UUID().uuidString)")
            defer { try? FileManager.default.removeItem(at: tempURL) }

            let result = try await client.downloadBucketFile(
                pick,
                in: id,
                to: tempURL
            )
            #expect(result == tempURL)

            let attrs = try FileManager.default.attributesOfItem(atPath: tempURL.path)
            let actualSize = (attrs[.size] as? Int64) ?? -1
            #expect(actualSize == pick.size)
        }

        @Test(
            "Create and delete a bucket under the default namespace",
            .enabled(if: runE2E && hasToken)
        )
        func createAndDeleteBucket() async throws {
            // Pick up HF_TOKEN from the environment.
            let client = HubClient()

            let suffix = UUID().uuidString.prefix(8).lowercased()
            let name = "swift-hf-e2e-\(suffix)"
            let (url, returnedId) = try await client.createBucket(name: name)

            #expect(url.contains(name))
            // Canonical id should be `<resolved-username>/<name>` — not "me".
            #expect(returnedId?.hasSuffix("/\(name)") == true)
            #expect(returnedId?.hasPrefix("me/") == false)

            // Subsequent reads/deletes use the canonical id
            guard let bucketIDString = returnedId,
                let id = Bucket.ID(rawValue: bucketIDString)
            else {
                Issue.record("createBucket did not return a parseable canonical id")
                return
            }

            // Round-trip via getBucket to verify the bucket really exists.
            let info = try? await client.getBucket(id)
            #expect(info != nil)
            #expect(info?.id.name == name)

            // Cleanup. missingOk=true so a flaky-test residual still succeeds
            try await client.deleteBucket(id, missingOk: true)
        }
    }

#endif  // HUGGINGFACE_ENABLE_XET
