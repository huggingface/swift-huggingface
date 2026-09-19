import Foundation

extension HubClient {
    /// Fetches the next page of results for a paginated response.
    ///
    /// Retry a failed request by passing the same page again.
    /// Links must use the client's origin (scheme, host, and port).
    ///
    /// - Parameter page: The current paginated response.
    /// - Returns: The next page of results,
    ///   or `nil` if there are no more pages.
    /// - Throws: An error if the request fails or is canceled.
    ///   Throws `HTTPClientError.requestError` for an unsafe or repeated link.
    public func nextPage<T: Decodable & Sendable>(
        after page: PaginatedResponse<T>
    ) async throws -> PaginatedResponse<T>? {
        try Task.checkCancellation()
        guard let next = page.nextURL else { return nil }
        let resolvedNextURL = resolveNextPageURL(next, requestURL: page.requestURL ?? host)
        guard hasSameOrigin(resolvedNextURL, host),
            var components = URLComponents(url: resolvedNextURL, resolvingAgainstBaseURL: true),
            components.user == nil, components.password == nil
        else {
            throw HTTPClientError.requestError("Pagination link must use the client's origin")
        }

        if let treeURL = page.treeRequestURL,
            let treeComponents = URLComponents(url: treeURL, resolvingAgainstBaseURL: true)
        {
            // Tree links supply the cursor; the original request defines the scope.
            components.percentEncodedPath = treeComponents.percentEncodedPath
            let originalItems = treeComponents.queryItems ?? []
            let originalNames = Set(originalItems.map(\.name))
            components.queryItems =
                (components.queryItems ?? []).filter {
                    !originalNames.contains($0.name)
                } + originalItems
        }
        components.fragment = nil
        guard let url = components.url else {
            throw HTTPClientError.requestError("Invalid pagination link")
        }
        var visitedURLs = page.visitedURLs
        if let requestURL = page.requestURL {
            visitedURLs.insert(paginationURLIdentity(requestURL))
        }
        guard visitedURLs.insert(paginationURLIdentity(url)).inserted else {
            throw HTTPClientError.requestError("Repeated pagination link")
        }
        var nextPage: PaginatedResponse<T> = try await httpClient.fetchPaginated(.get, url: url)
        nextPage.treeRequestURL = page.treeRequestURL
        nextPage.visitedURLs = visitedURLs
        return nextPage
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

// Compare origins before the HTTP client obtains or attaches credentials.
private func hasSameOrigin(_ lhs: URL, _ rhs: URL) -> Bool {
    func port(_ url: URL) -> Int? {
        url.port ?? (url.scheme?.lowercased() == "https" ? 443 : 80)
    }
    return lhs.scheme?.lowercased() == rhs.scheme?.lowercased()
        && lhs.host?.lowercased() == rhs.host?.lowercased()
        && port(lhs) == port(rhs)
}

// Query ordering and fragments do not identify a new page.
private func paginationURLIdentity(_ url: URL) -> URL {
    guard var components = URLComponents(url: url, resolvingAgainstBaseURL: true) else { return url }
    components.fragment = nil
    components.queryItems = components.queryItems?.sorted {
        ($0.name, $0.value ?? "") < ($1.name, $1.value ?? "")
    }
    return components.url ?? url
}
