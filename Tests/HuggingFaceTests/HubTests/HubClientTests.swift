import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

@Suite("Hub Client Tests")
struct HubClientTests {
    @Test("Client can be initialized with default configuration")
    func testDefaultClientInitialization() async {
        let client = HubClient.default
        #expect(client.host == URL(string: "https://huggingface.co/")!)
        #expect(client.userAgent == nil)
        let token = await client.bearerToken
        #expect(token == nil || token != nil)
    }

    @Test("Client can be initialized with custom configuration")
    func testCustomClientInitialization() async {
        let host = URL(string: "https://huggingface.co")!
        let userAgent = "TestApp/1.0"
        let bearerToken = "test_token"

        let client = HubClient(
            host: host,
            userAgent: userAgent,
            bearerToken: bearerToken
        )

        #expect(client.host.absoluteString.hasPrefix(host.absoluteString))
        #expect(client.userAgent == userAgent)
        #expect(await client.bearerToken == bearerToken)
    }

    @Test("Client normalizes host URL with trailing slash")
    func testHostNormalization() {
        let hostWithoutSlash = URL(string: "https://huggingface.co")!
        let client = HubClient(host: hostWithoutSlash)

        #expect(client.host.path.hasSuffix("/"))
    }

}
