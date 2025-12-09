import Foundation

// MARK: - Organizations API

extension HubClient {
    /// Lists organizations from the Hub.
    ///
    /// - Parameters:
    ///   - search: Search term to filter organizations.
    ///   - sort: Property to use when sorting (e.g., "createdAt").
    ///   - limit: Limit the number of organizations fetched.
    /// - Returns: A paginated response containing organization information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func listOrganizations(
        search: String? = nil,
        sort: String? = nil,
        limit: Int? = nil
    ) async throws -> PaginatedResponse<Organization> {
        var params: [String: Value] = [:]

        if let search { params["search"] = .string(search) }
        if let sort { params["sort"] = .string(sort) }
        if let limit { params["limit"] = .int(limit) }

        return try await httpClient.fetchPaginated(.get, "/api/organizations", params: params)
    }

    /// Gets information for a specific organization.
    ///
    /// - Parameter id: The organization's identifier (e.g., "huggingface").
    /// - Returns: Information about the organization.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func getOrganization(_ id: String) async throws -> Organization {
        return try await httpClient.fetch(.get, "/api/organizations/\(id)")
    }

    /// Lists members of an organization.
    ///
    /// Requires authentication with appropriate permissions.
    ///
    /// - Parameter id: The organization's identifier.
    /// - Returns: A list of organization members.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func listOrganizationMembers(_ id: String) async throws -> [Organization.Member] {
        return try await httpClient.fetch(.get, "/api/organizations/\(id)/members")
    }

    // MARK: - Organization Billing

    /// Gets organization billing usage for a given period.
    ///
    /// Requires authentication with appropriate permissions.
    ///
    /// - Parameters:
    ///   - name: The organization's name.
    ///   - periodId: Optional period ID to get usage for a specific period.
    /// - Returns: Organization billing usage information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func getOrganizationBillingUsage(
        name: String,
        periodId: String? = nil
    ) async throws -> Billing.Usage {
        var params: [String: Value] = [:]
        if let periodId { params["periodId"] = .string(periodId) }

        return try await httpClient.fetch(.get, "/api/organizations/\(name)/billing/usage", params: params)
    }

    /// Gets live organization billing usage.
    ///
    /// Requires authentication with appropriate permissions.
    ///
    /// - Parameter name: The organization's name.
    /// - Returns: Live organization billing usage information.
    /// - Throws: An error if the request fails or the response cannot be decoded.
    public func getOrganizationBillingUsageLive(name: String) async throws -> Billing.Usage {
        return try await httpClient.fetch(.get, "/api/organizations/\(name)/billing/usage/live")
    }

    // MARK: - Organization Resource Groups

    /// Creates a new resource group for an organization.
    ///
    /// Requires authentication with appropriate permissions.
    ///
    /// - Parameters:
    ///   - name: The organization's name.
    ///   - resourceGroupName: The name of the resource group to create.
    ///   - description: Optional description of the resource group.
    ///   - users: Dictionary mapping usernames to their roles.
    ///   - repos: Dictionary mapping repository names to their types (e.g., "model", "dataset").
    ///   - autoJoin: Auto-join configuration for the resource group.
    /// - Returns: `true` if the resource group was created successfully.
    /// - Throws: An error if the request fails.
    public func createOrganizationResourceGroup(
        name: String,
        resourceGroupName: String,
        description: String? = nil,
        users: [String: ResourceGroup.Role]? = nil,
        repos: [String: String]? = nil,
        autoJoin: ResourceGroup.AutoJoin? = nil
    ) async throws -> Bool {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: "organizations")
            .appending(path: name)
            .appending(path: "resource-groups")
        var params: [String: Value] = ["name": .string(resourceGroupName)]
        if let description { params["description"] = .string(description) }
        if let users {
            params["users"] = .array(
                users.map { username, role in
                    .object([
                        "user": .string(username),
                        "role": .string(role.rawValue),
                    ])
                }
            )
        }
        if let repos {
            params["repos"] = .array(
                repos.map { name, type in
                    .object([
                        "type": .string(type),
                        "name": .string(name),
                    ])
                }
            )
        }
        if let autoJoin {
            var auto: [String: Value] = ["enabled": .bool(autoJoin.enabled)]
            if let role = autoJoin.role { auto["role"] = .string(role.rawValue) }
            params["autoJoin"] = .object(auto)
        }
        let result: Bool = try await httpClient.fetch(.post, url: url, params: params)
        return result
    }
}
