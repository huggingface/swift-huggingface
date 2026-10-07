import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

@Suite("HTTPClientError")
struct HTTPClientErrorTests {
    private let response = HTTPURLResponse(
        url: URL(string: "https://huggingface.co/api/models/missing")!,
        statusCode: 404,
        httpVersion: "HTTP/1.1",
        headerFields: nil
    )!

    @Test func responseErrorsCarryTheResponse() {
        let error = HTTPClientError.responseError(response: response, detail: "Not found")
        #expect(error.code == .responseError)
        #expect(error.statusCode == 404)
        #expect(error.response == response)
        #expect(error.detail == "Not found")
        #expect(error.description == "Response error (Status 404): Not found")
    }

    @Test func decodingErrorsCarryTheResponse() {
        let error = HTTPClientError.decodingError(response: response, detail: "Bad JSON")
        #expect(error.code == .decodingError)
        #expect(error.statusCode == 404)
        #expect(error.description == "Decoding error (Status 404): Bad JSON")
    }

    @Test func requestAndUnexpectedErrorsHaveNoResponse() {
        let request = HTTPClientError.requestError("Invalid link")
        #expect(request.code == .requestError)
        #expect(request.response == nil)
        #expect(request.statusCode == nil)
        #expect(request.description == "Request error: Invalid link")

        let unexpected = HTTPClientError.unexpectedError("Invalid response")
        #expect(unexpected.code == .unexpectedError)
        #expect(unexpected.description == "Unexpected error: Invalid response")
    }

    @Test func codesCanBeMatched() {
        let error = HTTPClientError.responseError(response: response, detail: "Not found")
        switch error.code {
        case .responseError:
            break
        default:
            Issue.record("Expected a response error")
        }
        #expect(HTTPClientError.Code.responseError.description == "responseError")
    }
}
