//
//  File 2.swift
//  swift-huggingface
//
//  Created by Ronald Mannak on 12/17/25.
//

import Foundation

extension HubClient {
    public func nextPage<T: Decodable>(
        of page: PaginatedResponse<T>
    ) async throws -> PaginatedResponse<T>? {
        guard let next = page.nextURL else { return nil }
        return try await httpClient.fetchPaginated(.get, url: next)
    }
}
