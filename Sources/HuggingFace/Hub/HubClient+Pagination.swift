import Foundation

extension HubClient {
    /// Fetches the next page of results for a paginated response.
    ///
    /// - Parameter page: The current paginated response.
    /// - Returns: The next page of results,
    ///   or `nil` if there are no more pages.
    public func nextPage<T: Decodable>(
        after page: PaginatedResponse<T>
    ) async throws -> PaginatedResponse<T>? {
        guard let next = page.nextURL else { return nil }
        return try await httpClient.fetchPaginated(.get, url: next)
    }
}
