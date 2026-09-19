import Foundation
import HuggingFace
import Replay
import Testing

@Suite("Hub tags replay", .serialized, .playbackIsolated(replaysFrom: Bundle.module))
struct TagsReplayTests {
    #if canImport(FoundationNetworking)
        // Linux URLProtocol does not receive the session header used by test scope.
        private static let replayScope: ReplayScope = .global
    #else
        // Global registration can intercept concurrent Xet requests through URLSession.shared.
        private static let replayScope: ReplayScope = .test
    #endif

    private var client: HubClient {
        HubClient(
            session: Replay.session,
            host: URL(string: "https://huggingface.co")!,
            bearerToken: nil,
            cache: nil
        )
    }

    private static func filters(keeping selection: [String: Set<String>]) -> [Filter] {
        [
            .custom { entry in
                var entry = entry
                let headers = ["authorization", "proxy-authorization", "cookie", "set-cookie"]
                entry.request.headers.removeAll { headers.contains($0.name.lowercased()) }
                entry.response.headers.removeAll { headers.contains($0.name.lowercased()) }
                entry.request.cookies = []
                entry.response.cookies = []
                // Keep complete entries and the API's top-level group structure.
                guard let text = entry.response.content.text,
                    entry.response.content.encoding == nil,
                    let groups = try? JSONSerialization.jsonObject(with: Data(text.utf8))
                        as? [String: [[String: Any]]]
                else {
                    Issue.record("Expected an unwrapped JSON object of tag groups when recording.")
                    return entry
                }
                var reduced: [String: [[String: Any]]] = [:]
                for (group, ids) in selection {
                    if let entries = groups[group] {
                        reduced[group] = entries.filter { tag in
                            guard let id = tag["id"] as? String else { return false }
                            return ids.contains(id)
                        }
                    }
                }
                guard let data = try? JSONSerialization.data(withJSONObject: reduced, options: [.sortedKeys]) else {
                    Issue.record("Could not encode reduced tag groups.")
                    return entry
                }
                entry.response.content.text = String(decoding: data, as: UTF8.self)
                entry.response.content.size = data.count
                entry.response.content.compression = nil
                entry.response.content.comment =
                    "Reduced recording: selected groups and tag IDs; complete entries retained."
                entry.response.bodySize = data.count
                entry.response.headersSize = -1
                let staleHeaders = ["content-length", "content-encoding", "etag", "content-md5", "digest"]
                entry.response.headers.removeAll { staleHeaders.contains($0.name.lowercased()) }
                return entry
            }
        ]
    }

    @Test(
        .replay(
            "model-tags",
            matching: [.method, .url],
            filters: filters(keeping: ["library": ["pytorch", "transformers"], "pipeline_tag": ["text-generation"]]),
            scope: replayScope
        )
    )
    func modelTags() async throws {
        let tags = try await client.getModelTags()
        let libraries = try #require(tags["library"])
        #expect(libraries.contains { $0.id == "pytorch" && $0.label == "PyTorch" })
        let tasks = try #require(tags["pipeline_tag"])
        #expect(tasks.contains { $0.id == "text-generation" })
    }

    @Test(
        .replay(
            "dataset-tags",
            matching: [.method, .url],
            filters: filters(keeping: ["library": ["library:datasets"], "language": ["language:en"]]),
            scope: replayScope
        )
    )
    func datasetTags() async throws {
        let tags = try await client.getDatasetTags()
        let libraries = try #require(tags["library"])
        #expect(libraries.contains { $0.id == "library:datasets" && !$0.label.isEmpty })
        let languages = try #require(tags["language"])
        #expect(languages.contains { $0.id == "language:en" })
    }
}
