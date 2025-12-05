import Foundation

// MARK: - Datasets API

extension HubClient {
    /// Lists datasets from the Hub.
    ///
    /// - Parameters:
    ///   - search: Filter based on substrings for repos and their usernames.
    ///   - author: Filter datasets by an author or organization.
    ///   - filter: Filter based on tags (e.g., "task_categories:text-classification").
    ///   - sort: Property to use when sorting (e.g., "downloads", "author").
    ///   - direction: Direction in which to sort.
    ///   - limit: Limit the number of datasets fetched.
    ///   - full: Whether to fetch most dataset data, such as all tags, the files, etc.
    ///   - config: Whether to also fetch the repo config.
    /// - Returns: A paginated response containing dataset information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func listDatasets(
        search: String? = nil,
        author: String? = nil,
        filter: String? = nil,
        sort: String? = nil,
        direction: SortDirection? = nil,
        limit: Int? = nil,
        full: Bool? = nil,
        config: Bool? = nil
    ) async throws -> PaginatedResponse<Dataset> {
        var params: [String: Value] = [:]

        if let search { params["search"] = .string(search) }
        if let author { params["author"] = .string(author) }
        if let filter { params["filter"] = .string(filter) }
        if let sort { params["sort"] = .string(sort) }
        if let direction { params["direction"] = .int(direction.rawValue) }
        if let limit { params["limit"] = .int(limit) }
        if let full { params["full"] = .bool(full) }
        if let config { params["config"] = .bool(config) }

        return try await httpClient.fetchPaginated(.get, "/api/datasets", params: params)
    }

    /// Gets information for a specific dataset.
    ///
    /// - Parameters:
    ///   - id: The repository identifier (e.g., "datasets/squad").
    ///   - revision: The git revision (branch, tag, or commit hash). If nil, uses the repo's default branch (usually "main").
    ///   - full: Whether to fetch most dataset data.
    /// - Returns: Information about the dataset.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func getDataset(
        _ id: Repo.ID,
        revision: String? = nil,
        full: Bool? = nil
    ) async throws -> Dataset {
        var url = httpClient.host
            .appending(path: "api")
            .appending(path: "datasets")
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

    /// Gets all available dataset tags hosted in the Hub.
    ///
    /// - Returns: Tag information organized by type.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func getDatasetTags() async throws -> Tags {
        return try await httpClient.fetch(.get, "/api/datasets-tags-by-type")
    }

    /// Lists Parquet files for a dataset.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - subset: Optional subset/configuration name.
    ///   - split: Optional split name.
    /// - Returns: List of Parquet file information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func listParquetFiles(
        _ id: Repo.ID,
        subset: String? = nil,
        split: String? = nil
    ) async throws -> [ParquetFileInfo] {
        var path = "/api/datasets/\(id.namespace)/\(id.name)/parquet"

        if let subset {
            path += "/\(subset)"
            if let split {
                path += "/\(split)"
            }
        }

        return try await httpClient.fetch(.get, path)
    }

    // MARK: - Dataset Access Requests

    /// Requests access to a gated dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - reason: The reason for requesting access.
    ///   - institution: The institution associated with the request.
    /// - Returns: `true` if the request was submitted successfully.
    /// - Throws: An error if the request fails.
    public func requestDatasetAccess(
        _ id: Repo.ID,
        reason: String? = nil,
        institution: String? = nil
    ) async throws -> Bool {
        let path = "/datasets/\(id.namespace)/\(id.name)/ask-access"
        var params: [String: Value] = [:]
        if let reason { params["reason"] = .string(reason) }
        if let institution { params["institution"] = .string(institution) }
        let result: Bool = try await httpClient.fetch(.post, path, params: params)
        return result
    }

    /// Cancels the current user's access request to a gated dataset repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: `true` if the request was cancelled successfully.
    /// - Throws: An error if the request fails.
    public func cancelDatasetAccessRequest(_ id: Repo.ID) async throws -> Bool {
        let path = "/api/datasets/\(id.namespace)/\(id.name)/user-access-request/cancel"
        let result: Bool = try await httpClient.fetch(.post, path)
        return result
    }

    /// Grants access to a user for a gated dataset repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: `true` if access was granted successfully.
    /// - Throws: An error if the request fails.
    public func grantDatasetAccess(_ id: Repo.ID) async throws -> Bool {
        let path = "/api/datasets/\(id.namespace)/\(id.name)/user-access-request/grant"
        let result: Bool = try await httpClient.fetch(.post, path)
        return result
    }

    /// Handles an access request for a gated dataset repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: `true` if the request was handled successfully.
    /// - Throws: An error if the request fails.
    public func handleDatasetAccessRequest(_ id: Repo.ID) async throws -> Bool {
        let path = "/api/datasets/\(id.namespace)/\(id.name)/user-access-request/handle"
        let result: Bool = try await httpClient.fetch(.post, path)
        return result
    }

    /// Lists access requests for a gated dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - status: The status to filter by ("pending", "accepted", "rejected").
    /// - Returns: A list of access requests.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func listDatasetAccessRequests(
        _ id: Repo.ID,
        status: AccessRequest.Status
    ) async throws -> [AccessRequest] {
        let path = "/api/datasets/\(id.namespace)/\(id.name)/user-access-request/\(status.rawValue)"
        return try await httpClient.fetch(.get, path)
    }

    /// Gets user access report for a dataset repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: User access report data.
    /// - Throws: An error if the request fails.
    public func getDatasetUserAccessReport(_ id: Repo.ID) async throws -> Data {
        let path = "/datasets/\(id.namespace)/\(id.name)/user-access-report"
        return try await httpClient.fetchData(.get, path)
    }

    // MARK: - Dataset Advanced Features

    /// Sets the resource group for a dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - resourceGroupId: The resource group ID to set, or nil to unset.
    /// - Returns: Resource group response information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func setDatasetResourceGroup(
        _ id: Repo.ID,
        resourceGroupId: String?
    ) async throws -> ResourceGroup {
        let path = "/api/datasets/\(id.namespace)/\(id.name)/resource-group"

        let params: [String: Value] = [
            "resourceGroupId": resourceGroupId.map { .string($0) } ?? .null
        ]

        return try await httpClient.fetch(.post, path, params: params)
    }

    /// Scans a dataset repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: `true` if the scan was initiated successfully.
    /// - Throws: An error if the request fails.
    public func scanDataset(_ id: Repo.ID) async throws -> Bool {
        let path = "/api/datasets/\(id.namespace)/\(id.name)/scan"
        let result: Bool = try await httpClient.fetch(.post, path)
        return result
    }

    /// Creates a new tag for a dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision to tag.
    ///   - tag: The name of the tag to create.
    ///   - message: An optional message for the tag.
    /// - Returns: `true` if the tag was created successfully.
    /// - Throws: An error if the request fails.
    public func createDatasetTag(
        _ id: Repo.ID,
        revision: String,
        tag: String,
        message: String? = nil
    ) async throws -> Bool {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "datasets")
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

    /// Super-squashes commits in a dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision to squash.
    ///   - message: The commit message for the squashed commit.
    /// - Returns: The new commit ID.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func superSquashDataset(
        _ id: Repo.ID,
        revision: String,
        message: String
    ) async throws -> String {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "datasets")
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
