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
