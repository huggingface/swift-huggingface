import Foundation

extension HubClient {
    /// Fetches the next page of results for a paginated response.
    ///
    /// - Parameter page: The current paginated response.
    /// - Returns: The next page of results,
    ///   or `nil` if there are no more pages.
    public func nextPage<T: Decodable & Sendable>(
        after page: PaginatedResponse<T>
    ) async throws -> PaginatedResponse<T>? {
        guard let next = page.nextURL else { return nil }
        return try await httpClient.fetchPaginated(.get, url: next)
    }
}

// MARK: - Pagination Convenience Methods

extension HubClient {
    /// Lists model pages from the Hub.
    ///
    /// This convenience method fetches the first page immediately and returns a lazy
    /// page sequence for subsequent pages.
    ///
    /// - Parameters:
    ///   - search: Filter based on substrings for repos and their usernames.
    ///   - author: Filter models by an author or organization.
    ///   - filter: Filter based on tags (e.g., "text-classification").
    ///   - sort: Property to use when sorting (e.g., "downloads", "author").
    ///   - direction: Direction in which to sort.
    ///   - perPage: Limit the number of models fetched per page.
    ///   - full: Whether to fetch most model data, such as all tags, the files, etc.
    ///   - config: Whether to also fetch the repo config.
    /// - Returns: A lazy sequence of model pages.
    /// - Throws: An error if fetching the first page fails.
    public func listAllModels(
        search: String? = nil,
        author: String? = nil,
        filter: String? = nil,
        sort: String? = nil,
        direction: SortDirection? = nil,
        perPage: Int? = nil,
        full: Bool? = nil,
        config: Bool? = nil
    ) async throws -> Pages<Model> {
        let firstPage = try await listModels(
            search: search,
            author: author,
            filter: filter,
            sort: sort,
            direction: direction,
            limit: perPage,
            full: full,
            config: config
        )
        return Pages(firstPage: firstPage) { [self] page in
            try await nextPage(after: page)
        }
    }

    /// Lists dataset pages from the Hub.
    ///
    /// This convenience method fetches the first page immediately and returns a lazy
    /// page sequence for subsequent pages.
    ///
    /// - Parameters:
    ///   - search: Filter based on substrings for repos and their usernames.
    ///   - author: Filter datasets by an author or organization.
    ///   - filter: Filter based on tags (e.g., "task_categories:text-classification").
    ///   - sort: Property to use when sorting (e.g., "downloads", "author").
    ///   - direction: Direction in which to sort.
    ///   - perPage: Limit the number of datasets fetched per page.
    ///   - full: Whether to fetch most dataset data, such as all tags, the files, etc.
    ///   - config: Whether to also fetch the repo config.
    /// - Returns: A lazy sequence of dataset pages.
    /// - Throws: An error if fetching the first page fails.
    public func listAllDatasets(
        search: String? = nil,
        author: String? = nil,
        filter: String? = nil,
        sort: String? = nil,
        direction: SortDirection? = nil,
        perPage: Int? = nil,
        full: Bool? = nil,
        config: Bool? = nil
    ) async throws -> Pages<Dataset> {
        let firstPage = try await listDatasets(
            search: search,
            author: author,
            filter: filter,
            sort: sort,
            direction: direction,
            limit: perPage,
            full: full,
            config: config
        )
        return Pages(firstPage: firstPage) { [self] page in
            try await nextPage(after: page)
        }
    }

    /// Lists space pages from the Hub.
    ///
    /// This convenience method fetches the first page immediately and returns a lazy
    /// page sequence for subsequent pages.
    ///
    /// - Parameters:
    ///   - search: Filter based on substrings for repos and their usernames.
    ///   - author: Filter spaces by an author or organization.
    ///   - filter: Filter based on tags.
    ///   - sort: Property to use when sorting (e.g., "likes", "author").
    ///   - direction: Direction in which to sort.
    ///   - perPage: Limit the number of spaces fetched per page.
    ///   - full: Whether to fetch most space data, such as all tags, the files, etc.
    /// - Returns: A lazy sequence of space pages.
    /// - Throws: An error if fetching the first page fails.
    public func listAllSpaces(
        search: String? = nil,
        author: String? = nil,
        filter: String? = nil,
        sort: String? = nil,
        direction: SortDirection? = nil,
        perPage: Int? = nil,
        full: Bool? = nil
    ) async throws -> Pages<Space> {
        let firstPage = try await listSpaces(
            search: search,
            author: author,
            filter: filter,
            sort: sort,
            direction: direction,
            limit: perPage,
            full: full
        )
        return Pages(firstPage: firstPage) { [self] page in
            try await nextPage(after: page)
        }
    }

    /// Lists organization pages from the Hub.
    ///
    /// This convenience method fetches the first page immediately and returns a lazy
    /// page sequence for subsequent pages.
    ///
    /// - Parameters:
    ///   - search: Search term to filter organizations.
    ///   - sort: Property to use when sorting (e.g., "createdAt").
    ///   - perPage: Limit the number of organizations fetched per page.
    /// - Returns: A lazy sequence of organization pages.
    /// - Throws: An error if fetching the first page fails.
    public func listAllOrganizations(
        search: String? = nil,
        sort: String? = nil,
        perPage: Int? = nil
    ) async throws -> Pages<Organization> {
        let firstPage = try await listOrganizations(
            search: search,
            sort: sort,
            limit: perPage
        )
        return Pages(firstPage: firstPage) { [self] page in
            try await nextPage(after: page)
        }
    }

    /// Lists paper pages from the Hub.
    ///
    /// This convenience method fetches the first page immediately and returns a lazy
    /// page sequence for subsequent pages.
    ///
    /// - Parameters:
    ///   - search: Search term to filter papers.
    ///   - sort: Property to use when sorting (e.g., "trending", "updated").
    ///   - perPage: Limit the number of papers fetched per page.
    /// - Returns: A lazy sequence of paper pages.
    /// - Throws: An error if fetching the first page fails.
    public func listAllPapers(
        search: String? = nil,
        sort: String? = nil,
        perPage: Int? = nil
    ) async throws -> Pages<Paper> {
        let firstPage = try await listPapers(
            search: search,
            sort: sort,
            limit: perPage
        )
        return Pages(firstPage: firstPage) { [self] page in
            try await nextPage(after: page)
        }
    }

    /// Lists collection pages from the Hub.
    ///
    /// This convenience method fetches the first page immediately and returns a lazy
    /// page sequence for subsequent pages.
    ///
    /// - Parameters:
    ///   - owner: Filter collections by owner (username or organization).
    ///   - search: Search term to filter collections.
    ///   - sort: Property to use when sorting (e.g., "trending", "updated").
    ///   - perPage: Limit the number of collections fetched per page.
    /// - Returns: A lazy sequence of collection pages.
    /// - Throws: An error if fetching the first page fails.
    public func listAllCollections(
        owner: String? = nil,
        search: String? = nil,
        sort: String? = nil,
        perPage: Int? = nil
    ) async throws -> Pages<Collection> {
        let firstPage = try await listCollections(
            owner: owner,
            search: search,
            sort: sort,
            limit: perPage
        )
        return Pages(firstPage: firstPage) { [self] page in
            try await nextPage(after: page)
        }
    }
}
