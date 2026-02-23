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
    ///   - existOk: Whether to return success when the repository already exists.
    ///   - resourceGroupId: The resource group identifier to attach to the repository.
    ///   - space: Space-specific configuration for Space repositories.
    /// - Returns: A tuple containing the URL and repository ID of the created repository.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    /// - Important: Pass `space` when creating a Space repository.
    public func createRepo(
        kind: Repo.Kind,
        name: String,
        organization: String? = nil,
        visibility: Repo.Visibility = .public,
        existOk: Bool = false,
        resourceGroupId: String? = nil,
        space: Repo.SpaceConfiguration? = nil
    ) async throws -> (url: String, repoId: String?) {
        if kind == .space && space == nil && !existOk {
            throw HTTPClientError.requestError("space is required when creating a space repository")
        }

        var params: [String: Value] = [
            "type": .string(kind.rawValue),
            "name": .string(name),
            "private": .bool(visibility.isPrivate),
        ]

        if let organization {
            params["organization"] = .string(organization)
        }
        if let resourceGroupId {
            params["resourceGroupId"] = .string(resourceGroupId)
        }
        if kind == .space, let space {
            params["sdk"] = .string(space.sdk)
            if let spaceHardware = space.hardware {
                params["hardware"] = .string(spaceHardware)
            }
            if let spaceStorage = space.storage {
                params["storageTier"] = .string(spaceStorage)
            }
            if let spaceSleepTime = space.sleepTime {
                params["sleepTimeSeconds"] = .int(spaceSleepTime)
            }
            if let spaceSecrets = space.secrets {
                params["secrets"] = toObjectArray(spaceSecrets)
            }
            if let spaceVariables = space.variables {
                params["variables"] = toObjectArray(spaceVariables)
            }
        }

        do {
            let response: CreateResponse = try await httpClient.fetch(.post, "/api/repos/create", params: params)
            return (url: response.url, repoId: response.repoID)
        } catch let HTTPClientError.responseError(response, _) where existOk && response.statusCode == 409 {
            var url = httpClient.host
            if kind != .model {
                url = url.appending(path: kind.pluralized)
            }
            if let organization {
                url = url.appending(path: organization)
            }
            url = url.appending(path: name)
            return (url: url.absoluteString, repoId: nil)
        }
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

private struct CreateResponse: Decodable, Sendable {
    let url: String
    let repoID: String?

    private enum CodingKeys: String, CodingKey {
        case url
        case repoID = "repoId"
        case repoIDLegacy = "repoID"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        url = try container.decode(String.self, forKey: .url)
        repoID =
            try container.decodeIfPresent(String.self, forKey: .repoID)
            ?? container.decodeIfPresent(String.self, forKey: .repoIDLegacy)
    }
}

private func toObjectArray(_ entries: [Repo.SpaceConfiguration.Entry]) -> Value {
    .array(
        entries.map { entry in
            .object(
                [
                    "key": .string(entry.key),
                    "value": .string(entry.value),
                    "description": entry.description.map(Value.string) ?? .null,
                ]
            )
        }
    )
}
