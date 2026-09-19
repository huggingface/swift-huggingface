import Foundation

// MARK: - Git Operations API

extension HubClient {
    // MARK: - Tree Operations

    /// Lists one page of files and directories in a repository tree.
    ///
    /// Use ``nextPage(after:)`` to fetch subsequent pages.
    /// A failed page request can be retried with the same preceding page.
    /// Pagination keeps the endpoint prefix, revision, subtree, and recursion setting.
    ///
    /// - Parameters:
    ///   - repo: The repository identifier.
    ///   - kind: The repository kind.
    ///   - revision: The Git branch, tag, or commit hash.
    ///   - path: The subtree path relative to the repository root, or `nil` for the root.
    ///   - recursive: Whether to include entries in nested directories.
    /// - Returns: A page of tree entries with a link to the next page, if available.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func listTree(
        in repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main",
        path: String? = nil,
        recursive: Bool = false
    ) async throws -> PaginatedResponse<Git.TreeEntry> {
        try Task.checkCancellation()
        var url =
            host
            .appending(path: "api")
            .appending(component: kind.pluralized)
            .appending(component: repo.namespace)
            .appending(component: repo.name)
            .appending(path: "tree")
            .appending(component: revision)
        if let path, !path.isEmpty {
            url = url.appending(path: path)
        }
        var page: PaginatedResponse<Git.TreeEntry> = try await httpClient.fetchPaginated(
            .get,
            url: url,
            params: ["recursive": .bool(recursive)]
        )
        page.treeRequestURL = page.requestURL
        return page
    }

    /// Lists all pages of files and directories in a repository tree.
    ///
    /// The first page is fetched immediately.
    /// Subsequent pages are fetched as iteration advances.
    /// Use ``listTree(in:kind:revision:path:recursive:)`` and ``nextPage(after:)``
    /// when you need to retry individual page requests.
    ///
    /// - Parameters:
    ///   - repo: The repository identifier.
    ///   - kind: The repository kind.
    ///   - revision: The Git branch, tag, or commit hash.
    ///   - path: The subtree path relative to the repository root, or `nil` for the root.
    ///   - recursive: Whether to include entries in nested directories.
    /// - Returns: A sequence of tree pages.
    /// - Throws: An error if the first page cannot be fetched or decoded.
    ///   Iteration throws if a later request fails, is canceled,
    ///   or contains an unsafe or repeated pagination link.
    public func listAllTree(
        in repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main",
        path: String? = nil,
        recursive: Bool = false
    ) async throws -> Pages<Git.TreeEntry> {
        let firstPage = try await listTree(
            in: repo,
            kind: kind,
            revision: revision,
            path: path,
            recursive: recursive
        )
        return Pages(firstPage: firstPage) { [self] page in
            try await nextPage(after: page)
        }
    }

    /// Lists files and directories in a model repository tree.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash). Defaults to "main".
    ///   - path: The path within the repository. Defaults to root.
    /// - Returns: The first page of tree entries.
    ///   Use ``listAllTree(in:kind:revision:path:recursive:)`` for the complete listing.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func modelTree(
        _ id: Repo.ID,
        revision: String = "main",
        path: String? = nil
    ) async throws -> [Git.TreeEntry] {
        try await fetchTree(repoKind: .model, id: id, revision: revision, path: path)
    }

    /// Lists files and directories in a dataset repository tree.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash). Defaults to "main".
    ///   - path: The path within the repository. Defaults to root.
    /// - Returns: The first page of tree entries.
    ///   Use ``listAllTree(in:kind:revision:path:recursive:)`` for the complete listing.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func datasetTree(
        _ id: Repo.ID,
        revision: String = "main",
        path: String? = nil
    ) async throws -> [Git.TreeEntry] {
        try await fetchTree(repoKind: .dataset, id: id, revision: revision, path: path)
    }

    /// Lists files and directories in a space repository tree.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash). Defaults to "main".
    ///   - path: The path within the repository. Defaults to root.
    /// - Returns: The first page of tree entries.
    ///   Use ``listAllTree(in:kind:revision:path:recursive:)`` for the complete listing.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func spaceTree(
        _ id: Repo.ID,
        revision: String = "main",
        path: String? = nil
    ) async throws -> [Git.TreeEntry] {
        try await fetchTree(repoKind: .space, id: id, revision: revision, path: path)
    }

    private func fetchTree(
        repoKind: Repo.Kind,
        id: Repo.ID,
        revision: String,
        path: String?
    ) async throws -> [Git.TreeEntry] {
        try await listTree(in: id, kind: repoKind, revision: revision, path: path).items
    }

    // MARK: - Refs Operations

    /// Lists branches and tags for a model repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: A tuple containing arrays of branches and tags.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func modelRefs(_ id: Repo.ID) async throws -> (branches: [Git.Ref], tags: [Git.Ref]?) {
        try await fetchRefs(repoKind: .model, id: id)
    }

    /// Lists branches and tags for a dataset repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: A tuple containing arrays of branches and tags.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func datasetRefs(_ id: Repo.ID) async throws -> (branches: [Git.Ref], tags: [Git.Ref]?) {
        try await fetchRefs(repoKind: .dataset, id: id)
    }

    /// Lists branches and tags for a space repository.
    ///
    /// - Parameter id: The repository identifier.
    /// - Returns: A tuple containing arrays of branches and tags.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func spaceRefs(_ id: Repo.ID) async throws -> (branches: [Git.Ref], tags: [Git.Ref]?) {
        try await fetchRefs(repoKind: .space, id: id)
    }

    private func fetchRefs(
        repoKind: Repo.Kind,
        id: Repo.ID
    ) async throws -> (branches: [Git.Ref], tags: [Git.Ref]?) {
        let apiPath = "/api/\(repoKind.pluralized)/\(id.namespace)/\(id.name)/refs"
        let response: RefsResponse = try await httpClient.fetch(.get, apiPath)
        return (branches: response.branches, tags: response.tags)
    }

    // MARK: - Commits Operations

    /// Lists commits for a model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash). Defaults to "main".
    /// - Returns: An array of commits.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func modelCommits(
        _ id: Repo.ID,
        revision: String = "main"
    ) async throws -> [Git.Commit] {
        try await fetchCommits(repoKind: .model, id: id, revision: revision)
    }

    /// Lists commits for a dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash). Defaults to "main".
    /// - Returns: An array of commits.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func datasetCommits(
        _ id: Repo.ID,
        revision: String = "main"
    ) async throws -> [Git.Commit] {
        try await fetchCommits(repoKind: .dataset, id: id, revision: revision)
    }

    /// Lists commits for a space repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash). Defaults to "main".
    /// - Returns: An array of commits.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func spaceCommits(
        _ id: Repo.ID,
        revision: String = "main"
    ) async throws -> [Git.Commit] {
        try await fetchCommits(repoKind: .space, id: id, revision: revision)
    }

    private func fetchCommits(
        repoKind: Repo.Kind,
        id: Repo.ID,
        revision: String
    ) async throws -> [Git.Commit] {
        let apiPath = "/api/\(repoKind.pluralized)/\(id.namespace)/\(id.name)/commits/\(revision)"
        return try await httpClient.fetch(.get, apiPath)
    }

    // MARK: - File Download Operations

    /// Downloads a file from a model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash). Defaults to "main".
    ///   - path: The path to the file within the repository.
    /// - Returns: The file contents as `Data`.
    /// - Throws: An error if the request fails or the file cannot be downloaded.
    public func resolveModelFile(
        _ id: Repo.ID,
        revision: String = "main",
        path: String
    ) async throws -> Data {
        try await resolveFile(repoKind: .model, id: id, revision: revision, path: path)
    }

    /// Downloads a file from a dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash). Defaults to "main".
    ///   - path: The path to the file within the repository.
    /// - Returns: The file contents as `Data`.
    /// - Throws: An error if the request fails or the file cannot be downloaded.
    public func resolveDatasetFile(
        _ id: Repo.ID,
        revision: String = "main",
        path: String
    ) async throws -> Data {
        try await resolveFile(repoKind: .dataset, id: id, revision: revision, path: path)
    }

    /// Downloads a file from a space repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash). Defaults to "main".
    ///   - path: The path to the file within the repository.
    /// - Returns: The file contents as `Data`.
    /// - Throws: An error if the request fails or the file cannot be downloaded.
    public func resolveSpaceFile(
        _ id: Repo.ID,
        revision: String = "main",
        path: String
    ) async throws -> Data {
        try await resolveFile(repoKind: .space, id: id, revision: revision, path: path)
    }

    private func resolveFile(
        repoKind: Repo.Kind,
        id: Repo.ID,
        revision: String,
        path: String
    ) async throws -> Data {
        let prefix: String
        switch repoKind {
        case .model:
            prefix = ""
        case .dataset:
            prefix = "/datasets"
        case .space:
            prefix = "/spaces"
        }

        let filePath = "/\(id.namespace)/\(id.name)/resolve/\(revision)/\(path)"
        let fullPath = prefix + filePath

        return try await httpClient.fetchData(.get, fullPath)
    }

    // MARK: - Tree Size Operations

    /// Gets the total size of a model repository at a given revision, optionally under a specific subpath.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash).
    ///   - path: The path within the repository. Defaults to root.
    /// - Returns: Tree size information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func modelTreeSize(
        _ id: Repo.ID,
        revision: String = "main",
        path: String = ""
    ) async throws -> (path: String, size: Int) {
        try await fetchTreeSize(repoKind: .model, id: id, revision: revision, path: path)
    }

    /// Gets the total size of a dataset repository at a given revision, optionally under a specific subpath.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash).
    ///   - path: The path within the repository. Defaults to root.
    /// - Returns: Tree size information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func datasetTreeSize(
        _ id: Repo.ID,
        revision: String = "main",
        path: String = ""
    ) async throws -> (path: String, size: Int) {
        try await fetchTreeSize(repoKind: .dataset, id: id, revision: revision, path: path)
    }

    /// Gets the total size of a space repository at a given revision, optionally under a specific subpath.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - revision: The git revision (branch, tag, or commit hash).
    ///   - path: The path within the repository. Defaults to root.
    /// - Returns: Tree size information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func spaceTreeSize(
        _ id: Repo.ID,
        revision: String = "main",
        path: String = ""
    ) async throws -> (path: String, size: Int) {
        try await fetchTreeSize(repoKind: .space, id: id, revision: revision, path: path)
    }

    private func fetchTreeSize(
        repoKind: Repo.Kind,
        id: Repo.ID,
        revision: String,
        path: String
    ) async throws -> (path: String, size: Int) {
        let pathComponent = path.isEmpty ? "" : "/\(path)"
        let apiPath = "/api/\(repoKind.pluralized)/\(id.namespace)/\(id.name)/treesize/\(revision)\(pathComponent)"
        let response: TreeSizeResponse = try await httpClient.fetch(.get, apiPath)
        return (path: response.path, size: response.size)
    }

    // MARK: - Branch Operations

    /// Creates a new branch in a model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - branchName: The name of the branch to create.
    ///   - request: The branch creation request.
    /// - Returns: `true` if the branch was created successfully.
    /// - Throws: An error if the request fails.
    public func createModelBranch(
        _ id: Repo.ID,
        branchName: String,
        startingPoint: String? = nil,
        emptyBranch: Bool? = nil,
        overwrite: Bool? = nil
    ) async throws -> Bool {
        try await createBranch(
            repoKind: .model,
            id: id,
            branchName: branchName,
            startingPoint: startingPoint,
            emptyBranch: emptyBranch,
            overwrite: overwrite
        )
    }

    /// Creates a new branch in a dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - branchName: The name of the branch to create.
    ///   - request: The branch creation request.
    /// - Returns: `true` if the branch was created successfully.
    /// - Throws: An error if the request fails.
    public func createDatasetBranch(
        _ id: Repo.ID,
        branchName: String,
        startingPoint: String? = nil,
        emptyBranch: Bool? = nil,
        overwrite: Bool? = nil
    ) async throws -> Bool {
        try await createBranch(
            repoKind: .dataset,
            id: id,
            branchName: branchName,
            startingPoint: startingPoint,
            emptyBranch: emptyBranch,
            overwrite: overwrite
        )
    }

    /// Creates a new branch in a space repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - branchName: The name of the branch to create.
    ///   - request: The branch creation request.
    /// - Returns: `true` if the branch was created successfully.
    /// - Throws: An error if the request fails.
    public func createSpaceBranch(
        _ id: Repo.ID,
        branchName: String,
        startingPoint: String? = nil,
        emptyBranch: Bool? = nil,
        overwrite: Bool? = nil
    ) async throws -> Bool {
        try await createBranch(
            repoKind: .space,
            id: id,
            branchName: branchName,
            startingPoint: startingPoint,
            emptyBranch: emptyBranch,
            overwrite: overwrite
        )
    }

    private func createBranch(
        repoKind: Repo.Kind,
        id: Repo.ID,
        branchName: String,
        startingPoint: String?,
        emptyBranch: Bool?,
        overwrite: Bool?
    ) async throws -> Bool {
        let apiPath = "/api/\(repoKind.pluralized)/\(id.namespace)/\(id.name)/branch/\(branchName)"

        var params: [String: Value] = [:]
        if let startingPoint = startingPoint {
            params["startingPoint"] = .string(startingPoint)
        }
        if let emptyBranch = emptyBranch {
            params["emptyBranch"] = .bool(emptyBranch)
        }
        if let overwrite = overwrite {
            params["overwrite"] = .bool(overwrite)
        }

        let result: Bool = try await httpClient.fetch(.post, apiPath, params: params)
        return result
    }

    /// Deletes a branch from a model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - branchName: The name of the branch to delete.
    /// - Returns: `true` if the branch was deleted successfully.
    /// - Throws: An error if the request fails.
    public func deleteModelBranch(
        _ id: Repo.ID,
        branchName: String
    ) async throws -> Bool {
        try await deleteBranch(repoKind: .model, id: id, branchName: branchName)
    }

    /// Deletes a branch from a dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - branchName: The name of the branch to delete.
    /// - Returns: `true` if the branch was deleted successfully.
    /// - Throws: An error if the request fails.
    public func deleteDatasetBranch(
        _ id: Repo.ID,
        branchName: String
    ) async throws -> Bool {
        try await deleteBranch(repoKind: .dataset, id: id, branchName: branchName)
    }

    /// Deletes a branch from a space repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - branchName: The name of the branch to delete.
    /// - Returns: `true` if the branch was deleted successfully.
    /// - Throws: An error if the request fails.
    public func deleteSpaceBranch(
        _ id: Repo.ID,
        branchName: String
    ) async throws -> Bool {
        try await deleteBranch(repoKind: .space, id: id, branchName: branchName)
    }

    private func deleteBranch(
        repoKind: Repo.Kind,
        id: Repo.ID,
        branchName: String
    ) async throws -> Bool {
        let apiPath = "/api/\(repoKind.pluralized)/\(id.namespace)/\(id.name)/branch/\(branchName)"
        let result: Bool = try await httpClient.fetch(.delete, apiPath)
        return result
    }

    // MARK: - Compare Operations

    /// Compares two revisions in a model repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - compare: The comparison specification (e.g., "main...feature-branch").
    ///   - raw: Whether to return raw diff output.
    /// - Returns: The diff between the two revisions.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func compareModelRevisions(
        _ id: Repo.ID,
        compare: String,
        raw: Bool? = nil
    ) async throws -> String {
        try await compareRevisions(repoKind: .model, id: id, compare: compare, raw: raw)
    }

    /// Compares two revisions in a dataset repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - compare: The comparison specification (e.g., "main...feature-branch").
    ///   - raw: Whether to return raw diff output.
    /// - Returns: The diff between the two revisions.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func compareDatasetRevisions(
        _ id: Repo.ID,
        compare: String,
        raw: Bool? = nil
    ) async throws -> String {
        try await compareRevisions(repoKind: .dataset, id: id, compare: compare, raw: raw)
    }

    /// Compares two revisions in a space repository.
    ///
    /// - Parameters:
    ///   - id: The repository identifier.
    ///   - compare: The comparison specification (e.g., "main...feature-branch").
    ///   - raw: Whether to return raw diff output.
    /// - Returns: The diff between the two revisions.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func compareSpaceRevisions(
        _ id: Repo.ID,
        compare: String,
        raw: Bool? = nil
    ) async throws -> String {
        try await compareRevisions(repoKind: .space, id: id, compare: compare, raw: raw)
    }

    private func compareRevisions(
        repoKind: Repo.Kind,
        id: Repo.ID,
        compare: String,
        raw: Bool?
    ) async throws -> String {
        let apiPath = "/api/\(repoKind.pluralized)/\(id.namespace)/\(id.name)/compare/\(compare)"

        var params: [String: Value] = [:]
        if let raw = raw {
            params["raw"] = .bool(raw)
        }

        return try await httpClient.fetch(.get, apiPath, params: params)
    }
}

// MARK: - Private Response Types

private struct RefsResponse: Codable, Sendable {
    let branches: [Git.Ref]
    let tags: [Git.Ref]?
}

private struct TreeSizeResponse: Codable, Sendable {
    let path: String
    let size: Int
}
