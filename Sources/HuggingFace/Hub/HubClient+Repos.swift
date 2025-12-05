import Foundation

// MARK: - Repos API

extension HubClient {

    /// Creates a new repository on the Hub.
    ///
    /// - Parameters:
    ///   - kind: The kind of repository to create.
    ///   - name: The name of the repository.
    ///   - organization: The organization to create the repository under (optional).
    ///   - visibility: The visibility of the repository.
    /// - Returns: A tuple containing the URL and repository ID of the created repository.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func createRepo(
        kind: Repo.Kind,
        name: String,
        organization: String? = nil,
        visibility: Repo.Visibility = .public
    ) async throws -> (url: String, repoId: String?) {
        var params: [String: Value] = [
            "type": .string(kind.rawValue),
            "name": .string(name),
            "private": .bool(visibility.isPrivate),
        ]

        if let organization {
            params["organization"] = .string(organization)
        }

        let response: CreateResponse = try await httpClient.fetch(.post, "/api/repos/create", params: params)
        return (url: response.url, repoId: response.repoID)
    }

    /// Updates the settings of a repository.
    ///
    /// - Parameters:
    ///   - kind: The kind of repository.
    ///   - id: The repository identifier.
    ///   - settings: The settings to update.
    /// - Returns: `true` if the settings were successfully updated.
    /// - Throws: An error if the request fails.
    public func updateRepoSettings(
        kind: Repo.Kind,
        _ id: Repo.ID,
        settings: Repo.Settings
    ) async throws -> Bool {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: kind.pluralized)
            .appending(path: id.namespace)
            .appending(path: id.name)
            .appending(path: "settings")

        let encoder = JSONEncoder()
        let data = try encoder.encode(settings)
        let params = try JSONDecoder().decode([String: Value].self, from: data)

        return try await httpClient.fetch(.put, url: url, params: params)
    }

    /// Moves a repository to a new location.
    ///
    /// - Parameters:
    ///   - kind: The kind of repository.
    ///   - from: The current repository identifier.
    ///   - to: The new repository identifier.
    /// - Returns: `true` if the repository was successfully moved.
    /// - Throws: An error if the request fails.
    public func moveRepo(
        kind: Repo.Kind,
        from: Repo.ID,
        to: Repo.ID
    ) async throws -> Bool {
        let params: [String: Value] = [
            "fromRepo": .string(from.rawValue),
            "toRepo": .string(to.rawValue),
            "type": .string(kind.rawValue),
        ]

        return try await httpClient.fetch(.post, "/api/repos/move", params: params)
    }
}

// MARK: - Private Response Types

private struct CreateResponse: Codable, Sendable {
    let url: String
    let repoID: String?
}
