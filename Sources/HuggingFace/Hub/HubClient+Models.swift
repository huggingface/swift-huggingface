import Foundation

// MARK: - Models API

public extension HubClient {
    /// Lists models from the Hub with automatic pagination.
    ///
    /// This method returns an async sequence that automatically handles pagination,
    /// yielding individual models as they are fetched:
    /// ```swift
    /// for try await model in client.listAllModels(author: "google") {
    ///     print(model.name)
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - search: Filter based on substrings for repos and their usernames.
    ///   - author: Filter models by an author or organization.
    ///   - filter: Filter based on tags (e.g., "text-classification").
    ///   - sort: Property to use when sorting (e.g., "downloads", "author").
    ///   - direction: Direction in which to sort.
    ///   - limit: Maximum total number of models to return across all pages. This also sets the
///            API's per-page size for efficiency. For example, `limit: 100` returns at most
///            100 models total (matching Python's `list_models(limit=100)` behavior).
    ///   - full: Whether to fetch most model data, such as all tags, the files, etc.
    ///   - config: Whether to also fetch the repo config.
    /// - Returns: An async sequence of models.
    func listAllModels(
        search: String? = nil,
        author: String? = nil,
        filter: String? = nil,
        sort: String? = nil,
        direction: SortDirection? = nil,
        limit: Int? = nil,
        full: Bool? = nil,
        config: Bool? = nil
    ) -> PaginatedSequence<Model> {
        var params: [String: Value] = [:]

        if let search { params["search"] = .string(search) }
        if let author { params["author"] = .string(author) }
        if let filter { params["filter"] = .string(filter) }
        if let sort { params["sort"] = .string(sort) }
        if let direction { params["direction"] = .int(direction.rawValue) }
        if let limit { params["limit"] = .int(limit) }
        if let full { params["full"] = .bool(full) }
        if let config { params["config"] = .bool(config) }

        let capturedParams = params
        return PaginatedSequence(
            limit: limit,
            firstPage: { [httpClient] in
                try await httpClient.fetchPaginated(.get, "/api/models", params: capturedParams)
            },
            nextPage: { [httpClient] url in
                try await httpClient.fetchPaginated(.get, url: url)
            }
        )
    }

    /// Lists models from the Hub (single page).
    ///
    /// - Parameters:
    ///   - search: Filter based on substrings for repos and their usernames.
    ///   - author: Filter models by an author or organization.
    ///   - filter: Filter based on tags (e.g., "text-classification").
    ///   - sort: Property to use when sorting (e.g., "downloads", "author").
    ///   - direction: Direction in which to sort.
    ///   - limit: Limit the number of models fetched.
    ///   - full: Whether to fetch most model data, such as all tags, the files, etc.
    ///   - config: Whether to also fetch the repo config.
    /// - Returns: A paginated response containing model information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    @available(
        *,
        deprecated,
        message:
            "Use listAllModels() instead, which provides automatic pagination matching huggingface_hub's list_models()."
    )
    func listModels(
        search: String? = nil,
        author: String? = nil,
        filter: String? = nil,
        sort: String? = nil,
        direction: SortDirection? = nil,
        limit: Int? = nil,
        full: Bool? = nil,
        config: Bool? = nil
    ) async throws -> PaginatedResponse<Model> {
        var params: [String: Value] = [:]

        if let search { params["search"] = .string(search) }
        if let author { params["author"] = .string(author) }
        if let filter { params["filter"] = .string(filter) }
        if let sort { params["sort"] = .string(sort) }
        if let direction { params["direction"] = .int(direction.rawValue) }
        if let limit { params["limit"] = .int(limit) }
        if let full { params["full"] = .bool(full) }
        if let config { params["config"] = .bool(config) }

        return try await httpClient.fetchPaginated(.get, "/api/models", params: params)
    }

    /// Gets information for a specific model.
    ///
    /// - Parameters:
    ///   - id: The repository identifier (e.g., "facebook/bart-large-cnn").
    ///   - revision: The git revision (branch, tag, or commit hash). If nil, uses the repo's default branch (usually "main").
    ///   - full: Whether to fetch most model data.
    /// - Returns: Information about the model.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    func getModel(
        _ id: Repo.ID,
        revision: String? = nil,
        full: Bool? = nil
    ) async throws -> Model {
        var url = httpClient.host
            .appending(path: "api")
            .appending(path: "models")
            .appending(path: id.namespace)
            .appending(path: id.name)
        if let revision {
            url =
                url
                .appending(path: "revision")
                .appending(component: revision)
        }

        var params: [String: Value] = [:]
        if let full { params["full"] = .bool(full) }

        return try await httpClient.fetch(.get, url: url, params: params)
    }

    /// Gets all available model tags hosted in the Hub.
    ///
    /// - Returns: Tag information organized by type.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    func getModelTags() async throws -> Tags {
        return try await httpClient.fetch(.get, "/api/models-tags-by-type")
    }

    // MARK: - Model Access Requests

    /// Requests access to a gated model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - fields: Additional form fields to include with the access request (e.g., "reason", "institution").
    /// - Returns: `true` if the request was submitted successfully.
    /// - Throws: An error if the request fails.
    func requestModelAccess(
        _ id: Repo.ID,
        fields: [String: String]? = nil
    ) async throws -> Bool {
        let path = "/\(id.namespace)/\(id.name)/ask-access"
        let params: [String: Value] = fields?.mapValues { .string($0) } ?? [:]
        let result: Bool = try await httpClient.fetch(.post, path, params: params)
        return result
    }

    /// Cancels the current user's access request to a gated model repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: `true` if the request was cancelled successfully.
    /// - Throws: An error if the request fails.
    func cancelModelAccessRequest(_ id: Repo.ID) async throws -> Bool {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "models")
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "user-access-request")
            .appending(path: "cancel")
        let result: Bool = try await httpClient.fetch(.post, url: url)
        return result
    }

    /// Grants access to a user for a gated model repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: `true` if access was granted successfully.
    /// - Throws: An error if the request fails.
    func grantModelAccess(_ id: Repo.ID) async throws -> Bool {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "models")
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "user-access-request")
            .appending(path: "grant")
        let result: Bool = try await httpClient.fetch(.post, url: url)
        return result
    }

    /// Handles an access request for a gated model repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: `true` if the request was handled successfully.
    /// - Throws: An error if the request fails.
    func handleModelAccessRequest(_ id: Repo.ID) async throws -> Bool {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "models")
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "user-access-request")
            .appending(path: "handle")
        let result: Bool = try await httpClient.fetch(.post, url: url)
        return result
    }

    /// Lists access requests for a gated model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - status: The status to filter by ("pending", "accepted", "rejected").
    /// - Returns: A list of access requests.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    func listModelAccessRequests(
        _ id: Repo.ID,
        status: AccessRequest.Status
    ) async throws -> [AccessRequest] {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "models")
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "user-access-request")
            .appending(path: status.rawValue)
        return try await httpClient.fetch(.get, url: url)
    }

    /// Gets user access report for a model repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: User access report data.
    /// - Throws: An error if the request fails.
    func getModelUserAccessReport(_ id: Repo.ID) async throws -> Data {
        let url = httpClient.host
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "user-access-report")
        return try await httpClient.fetchData(.get, url: url)
    }

    // MARK: - Model Advanced Features

    /// Sets the resource group for a model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - resourceGroupId: The resource group ID to set, or nil to unset.
    /// - Returns: Resource group response information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    func setModelResourceGroup(
        _ id: Repo.ID,
        resourceGroupId: String?
    ) async throws -> ResourceGroup {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "models")
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "resource-group")

        let params: [String: Value] = [
            "resourceGroupId": resourceGroupId.map { .string($0) } ?? .null
        ]

        return try await httpClient.fetch(.post, url: url, params: params)
    }

    /// Scans a model repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: `true` if the scan was initiated successfully.
    /// - Throws: An error if the request fails.
    func scanModel(_ id: Repo.ID) async throws -> Bool {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "models")
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "scan")
        let result: Bool = try await httpClient.fetch(.post, url: url)
        return result
    }

    /// Creates a new tag for a model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision to tag.
    ///   - tag: The name of the tag to create.
    ///   - message: An optional message for the tag.
    /// - Returns: `true` if the tag was created successfully.
    /// - Throws: An error if the request fails.
    func createModelTag(
        _ id: Repo.ID,
        revision: String,
        tag: String,
        message: String? = nil
    ) async throws -> Bool {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "models")
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "tag")
            .appending(component: revision)

        let params: [String: Value] = [
            "tag": .string(tag),
            "message": message.map { .string($0) } ?? .null,
        ]

        let result: Bool = try await httpClient.fetch(.post, url: url, params: params)
        return result
    }

    /// Super-squashes commits in a model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision to squash.
    ///   - message: The commit message for the squashed commit.
    /// - Returns: The new commit ID.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    func superSquashModel(
        _ id: Repo.ID,
        revision: String,
        message: String
    ) async throws -> String {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "models")
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "super-squash")
            .appending(component: revision)

        let params: [String: Value] = [
            "message": .string(message)
        ]

        struct Response: Decodable { let commitID: String }
        let resp: Response = try await httpClient.fetch(.post, url: url, params: params)
        return resp.commitID
    }
}
