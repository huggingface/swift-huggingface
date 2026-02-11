# Swift Hugging Face

A Swift client for [Hugging Face](https://huggingface.co), providing access to both 
the [Hub API](https://huggingface.co/docs/hub/api) 
for managing models, datasets, and repositories, and 
the [Inference Providers API](https://huggingface.co/docs/inference-providers/index) 
for running AI tasks like chat completion, text-to-image generation, and more.

## Requirements

- Swift 6.0+
- macOS 14.0+ / iOS 17.0+ / watchOS 10.0+ / tvOS 17.0+ / visionOS 1.0+ / Linux

## Installation

### Swift Package Manager

Add the following to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/huggingface/swift-huggingface.git", from: "0.5.0")
]
```

## Usage

### Authentication

The Hugging Face API supports multiple authentication methods depending on your use case.

For development and CI/CD environments, tokens are automatically detected from
the environment and local files:

1. `HF_TOKEN` environment variable
2. `HUGGING_FACE_HUB_TOKEN` environment variable  
3. `HF_TOKEN_PATH` environment variable (path to token file)
4. `$HF_HOME/token` file
5. `~/.cache/huggingface/token` (standard HF CLI location)
6. `~/.huggingface/token` (fallback location)

```swift
import HuggingFace

// Automatically detects token from environment
let client = HubClient.default
let userInfo = try await client.whoami()
```

For user-facing applications that need to authenticate users:

```swift
import HuggingFace

// Create authentication manager
let authManager = try HuggingFaceAuthenticationManager(
    clientID: "your_client_id",
    redirectURL: URL(string: "yourapp://oauth/callback")!,
    scope: [.openid, .profile, .email],
    keychainService: "com.yourapp.huggingface",
    keychainAccount: "user_token"
)

// Sign in user
try await authManager.signIn()

// Use with clients
let client = InferenceClient(tokenProvider: .oauth(manager: authManager))
```

---

### Hub API

```swift
import HuggingFace

// Create a client (uses auto-detected credentials from environment)
let client = HubClient.default
```

#### Models

```swift
// List models
let models = try await client.listModels(
    search: "bert",
    author: "google",
    limit: 10
)

for model in models.items {
    print("\(model.id): \(model.downloads ?? 0) downloads")
}

// Get model information
let model = try await client.getModel("facebook/bart-large-cnn")
print("Model: \(model.id)")
print("Downloads: \(model.downloads ?? 0)")
print("Likes: \(model.likes ?? 0)")

// Get model tags
let tags = try await client.getModelTags()
```

#### Datasets

```swift
// List datasets
let datasets = try await client.listDatasets(
    filter: "task_categories:text-classification",
    sort: "downloads",
    limit: 20
)

// Get dataset information
let datasetInfo = try await client.getDataset("datasets/squad")

// List Parquet files for a dataset
let parquetFiles = try await client.listParquetFiles(
    "datasets/squad",
    subset: "plain_text",
    split: "train"
)

// Get dataset tags
let datasetTags = try await client.getDatasetTags()
```

#### Spaces

```swift
// List spaces
let spaces = try await client.listSpaces(
    author: "huggingface",
    sort: "likes",
    limit: 10
)

// Get space information
let spaceInfo = try await client.getSpace("stabilityai/stable-diffusion")

// Get space runtime information
let runtime = try await client.spaceRuntime("stabilityai/stable-diffusion")
print("Status: \(runtime.stage)")
print("Hardware: \(runtime.hardware ?? "unknown")")

// Manage spaces
_ = try await client.sleepSpace("user/my-space")
_ = try await client.restartSpace("user/my-space", factory: false)
```

#### Repository Management

```swift
// Create a repository
let repo = try await client.createRepo(
    kind: .model,
    name: "my-model",
    organization: nil,
    visibility: .public
)
print("Created: \(repo.url)")

// Update repository settings
let settings = Repo.Settings(
    visibility: .private,
    discussionsDisabled: false,
    gated: .manual
)
_ = try await client.updateRepoSettings(
    kind: .model,
    "user/my-model",
    settings: settings
)

// Move a repository
_ = try await client.moveRepo(
    kind: .model,
    from: "user/old-name",
    to: "user/new-name"
)
```

#### User Information

```swift
// Get current user information (requires authentication)
let userInfo = try await client.whoami()
print("Username: \(userInfo.name)")
print("Email: \(userInfo.email ?? "N/A")")
print("PRO: \(userInfo.isPro ?? false)")

if let organizations = userInfo.organizations {
    print("Organizations:")
    for org in organizations {
        print("  - \(org.name)")
    }
}
```

#### Collections

```swift
// List collections
let collections = try await client.listCollections(
    owner: "huggingface",
    sort: "trending",
    limit: 10
)

for collection in collections.items {
    print("\(collection.title): \(collection.upvotes ?? 0) upvotes")
}

// Get collection information
let collectionInfo = try await client.getCollection("user/my-collection")
print("Collection: \(collectionInfo.title)")
print("Items: \(collectionInfo.items?.count ?? 0)")

// Manage collection items
let item = Collection.Item(type: "model", id: "facebook/bart-large-cnn")
_ = try await client.addCollectionItem(
    namespace: "user",
    slug: "my-collection",
    id: "123",
    item: item,
    note: "Great model for summarization"
)

// Batch update collection items
let actions: [Collection.BatchAction] = [
    .update(
        id: "item123",
        data: .init(
            note: "Updated description",
            position: 1
        )
    )
]
_ = try await client.batchUpdateCollectionItems(
    namespace: "user",
    slug: "my-collection",
    id: "123",
    actions: actions
)

// Delete collection item
_ = try await client.deleteCollectionItem(
    namespace: "user",
    slug: "my-collection",
    id: "123",
    itemId: "item123"
)
```

#### Papers

```swift
// List papers
let papers = try await client.listPapers(
    search: "transformers",
    sort: "trending",
    limit: 10
)

for paper in papers.items {
    print("\(paper.title ?? "Untitled")")
    print("Authors: \(paper.authors?.joined(separator: ", ") ?? "Unknown")")
}

// Get paper information
let paperInfo = try await client.getPaper("2103.00020")
print("Title: \(paperInfo.title ?? "N/A")")
print("arXiv ID: \(paperInfo.arXivID ?? "N/A")")

// List daily papers
let dailyPapers = try await client.listDailyPapers(
    page: 1,
    limit: 20,
    sort: "trending"
)

for item in dailyPapers {
    print("\(item.title): \(item.paper.upvotes) upvotes")
    print("Authors: \(item.paper.authors?.joined(separator: ", ") ?? "Unknown")")
}
```

#### Organizations

```swift
// List organizations
let organizations = try await client.listOrganizations(
    search: "research",
    limit: 10
)

for org in organizations.items {
    print("\(org.name): \(org.numberOfModels ?? 0) models")
}

// Get organization information
let orgInfo = try await client.getOrganization("huggingface")
print("Organization: \(orgInfo.fullName ?? orgInfo.name)")
print("Members: \(orgInfo.numberOfMembers ?? 0)")
print("Models: \(orgInfo.numberOfModels ?? 0)")

// List organization members (requires authentication)
let members = try await client.listOrganizationMembers("huggingface")
for member in members {
    print("  - \(member.name) (\(member.role ?? "member"))")
}

// Get organization billing usage
let billingUsage = try await client.getOrganizationBillingUsage(name: "huggingface")
print("Billing period: \(billingUsage.period.periodStart) to \(billingUsage.period.periodEnd)")

// Get live billing usage
let liveUsage = try await client.getOrganizationBillingUsageLive(name: "huggingface")
print("Current usage: \(liveUsage.usage.count) entries")

// Create organization resource group
_ = try await client.createOrganizationResourceGroup(
    name: "huggingface",
    resourceGroupName: "research-team",
    description: "Research team resources",
    users: [
        "alice": .admin,
        "bob": .write
    ],
    repos: [
        "huggingface/repo": "dataset"
    ],
    autoJoin: ResourceGroup.AutoJoin(enabled: true, role: .read)
)
```

#### Discussions

```swift
// List discussions for a repository
let discussions = try await client.listDiscussions(
    kind: .model,
    "facebook/bart-large-cnn",
    page: 1,
    type: "pull_request",
    status: "open"
)

print("Found \(discussions.count) discussions")
for discussion in discussions.discussions {
    print("\(discussion.title) - \(discussion.status)")
}

// Get specific discussion
let discussion = try await client.getDiscussion(
    kind: .model,
    "facebook/bart-large-cnn",
    number: 1
)
print("Discussion: \(discussion.title)")

// Add comment to discussion
_ = try await client.addCommentToDiscussion(
    kind: .model,
    "facebook/bart-large-cnn",
    number: 1,
    comment: "Great work on this model!"
)

// Update discussion status
_ = try await client.updateDiscussionStatus(
    kind: .model,
    "facebook/bart-large-cnn",
    number: 1,
    status: Discussion.Status.closed
)
```

#### File Operations

```swift
// List files in a repository
let files = try await client.listFiles(
    in: "facebook/bart-large",
    kind: .model,
    revision: "main",
    recursive: true
)

for file in files {
    if file.type == .file {
        print("\(file.path) - \(file.size ?? 0) bytes")
    }
}

// Check if a file exists
let exists = await client.fileExists(
    at: "README.md",
    in: "facebook/bart-large"
)

// Get file information
let file = try await client.getFile(
    at: "pytorch_model.bin",
    in: "facebook/bart-large"
)
print("File size: \(file.size ?? 0)")
print("Is LFS: \(file.isLFS)")

// Download file data
let data = try await client.downloadContentsOfFile(
    at: "config.json",
    from: "openai-community/gpt2"
)
let config = try JSONDecoder().decode(ModelConfig.self, from: data)

// Download file to disk
let destination = FileManager.default.temporaryDirectory
    .appendingPathComponent("model.safetensors")

let fileURL = try await client.downloadFile(
    at: "model.safetensors",
    from: "openai-community/gpt2",
    to: destination
)

// Download with progress tracking
let progress = Progress(totalUnitCount: 0)
Task {
    for await _ in progress.values(forKeyPath: \.fractionCompleted) {
        print("Download progress: \(progress.fractionCompleted * 100)%")
    }
}

let fileURL = try await client.downloadFile(
    at: "pytorch_model.bin",
    from: "facebook/bart-large",
    to: destination,
    progress: progress
)

// Resume a download
let resumeData: Data = // ... from previous download
let fileURL = try await client.resumeDownloadFile(
    resumeData: resumeData,
    to: destination,
    progress: progress
)

// Upload a file
let result = try await client.uploadFile(
    URL(fileURLWithPath: "/path/to/local/file.csv"),
    to: "data/new_dataset.csv",
    in: "username/my-dataset",
    kind: .dataset,
    branch: "main",
    message: "Add new dataset"
)
print("Uploaded to: \(result.path)")

// Upload multiple files in a batch
let results = try await client.uploadFiles(
    [
        "README.md": .path("/path/to/readme.md"),
        "data.json": .path("/path/to/data.json"),
    ],
    to: "username/my-repo",
    message: "Initial commit",
    maxConcurrent: 3
)

// Or build a batch programmatically
var batch = FileBatch()
batch["config.json"] = .path("/path/to/config.json")
batch["model.safetensors"] = .url(
    URL(fileURLWithPath: "/path/to/model.safetensors"),
    mimeType: "application/octet-stream"
)

// Delete a file
try await client.deleteFile(
    at: "old_file.txt",
    from: "username/my-repo",
    message: "Remove old file"
)

// Delete multiple files
try await client.deleteFiles(
    at: ["file1.txt", "file2.txt", "old_dir/file3.txt"],
    from: "username/my-repo",
    message: "Cleanup old files"
)

// Download a complete repository snapshot
let snapshotDir = FileManager.default.temporaryDirectory
    .appendingPathComponent("models")
    .appendingPathComponent("facebook")
    .appendingPathComponent("bart-large")

let progress = Progress(totalUnitCount: 0)
Task {
    for await _ in progress.values(forKeyPath: \.fractionCompleted) {
        print("Snapshot progress: \(progress.fractionCompleted * 100)%")
    }
}

let destination = try await client.downloadSnapshot(
    of: "facebook/bart-large",
    kind: .model,
    to: snapshotDir,
    revision: "main",
    progressHandler: { progress in
        print("Downloaded \(progress.completedUnitCount) of \(progress.totalUnitCount) files")
    }
)
print("Repository downloaded to: \(destination.path)")

// Download only specific files using glob patterns
let destination = try await client.downloadSnapshot(
    of: "openai-community/gpt2",
    to: snapshotDir,
    matching: ["*.json", "*.txt"],  // Only download JSON and text files
    progressHandler: { progress in
        print("Progress: \(progress.fractionCompleted * 100)%")
    }
)
```

#### User Access Management

```swift
// Request access to a gated model
_ = try await client.requestModelAccess(
    "meta-llama/Llama-2-7b-hf",
    reason: "Research purposes",
    institution: "University of Example"
)

// List access requests for a model
let pendingRequests = try await client.listModelAccessRequests(
    "meta-llama/Llama-2-7b-hf",
    status: .pending
)

for request in pendingRequests {
    print("Request from: \(request.user.fullName)")
    print("Status: \(request.status)")
}

// Grant access to a user
_ = try await client.grantModelAccess(
    "meta-llama/Llama-2-7b-hf",
    username: "researcher123"
)

// Get user access report
let report = try await client.getModelUserAccessReport("meta-llama/Llama-2-7b-hf")
print("Access report size: \(report.count) bytes")
```

#### Space Monitoring and Management

```swift
// Stream space logs
let logStream = client.streamSpaceLogs("huggingface/stable-diffusion", logType: "build")
for try await logEntry in logStream {
    print("[\(logEntry.timestamp?.description ?? "-")] \(logEntry.message)")
}

// Stream space metrics
let metricsStream = client.streamSpaceMetrics("huggingface/stable-diffusion")
for try await metrics in metricsStream {
    print("CPU: \(metrics.cpuUsage ?? 0)% | Mem: \(metrics.memoryUsage ?? 0) bytes")
}

// Manage space secrets
_ = try await client.upsertSpaceSecret(
    "user/my-space",
    key: "API_KEY",
    description: "External API key",
    value: "secret_value"
)

// Delete space secret
_ = try await client.deleteSpaceSecret("user/my-space", key: "API_KEY")

// Manage space variables
_ = try await client.upsertSpaceVariable(
    "user/my-space",
    key: "MODEL_PATH",
    description: "Path to model files",
    value: "/models/stable-diffusion"
)
```

#### Pagination

The API automatically handles pagination using `Link` headers:

```swift
var page = try await client.listModels(limit: 100)
print("Page 1: \(page.items.count) models")

while page.nextURL != nil {
    guard let next = try await client.nextPage(after: page) else { break }
    page = next

    print("Page: \(page.items.count) models")
}
```

#### Error Handling

```swift
do {
    let modelInfo = try await client.getModel("nonexistent/model")
} catch let error as HTTPClientError {
    switch error {
    case .requestError(let detail):
        print("Request error: \(detail)")
    case .responseError(let response, let detail):
        print("Response error (\(response.statusCode)): \(detail)")
    case .decodingError(let response, let detail):
        print("Decoding error (\(response.statusCode)): \(detail)")
    case .unexpectedError(let detail):
        print("Unexpected error: \(detail)")
    }
}
```

<details>

<summary>Hub API Endpoint Coverage</summary>

##### Collections
- [x] `GET /api/collections` → `listCollections()`
- [x] `GET /api/collections/{namespace}/{slug}-{id}` → `getCollection()`
- [x] `POST /api/collections/{namespace}/{slug}-{id}/items` → `addCollectionItem()`
- [x] `POST /api/collections/{namespace}/{slug}-{id}/items/batch` → `batchUpdateCollectionItems()`
- [x] `DELETE /api/collections/{namespace}/{slug}-{id}/items/{itemId}` → `deleteCollectionItem()`

##### Datasets
- [x] `GET /api/datasets` → `listDatasets()`
- [x] `GET /api/datasets/{namespace}/{repo}` → `getDataset()`
- [x] `GET /api/datasets-tags-by-type` → `getDatasetTags()`
- [x] `GET /api/datasets/{namespace}/{repo}/parquet` → `listParquetFiles()`
- [ ] `GET /api/datasets/{namespace}/{repo}/branch/{rev}`
- [ ] `POST /api/datasets/{namespace}/{repo}/commit/{rev}`
- [x] `GET /api/datasets/{namespace}/{repo}/commits/{rev}` → `datasetCommits()`
- [x] `GET /api/datasets/{namespace}/{repo}/compare/{compare}` → `compareDatasetRevisions()`
- [ ] `GET /api/datasets/{namespace}/{repo}/lfs-files`
- [ ] `POST /api/datasets/{namespace}/{repo}/lfs-files/batch`
- [ ] `GET /api/datasets/{namespace}/{repo}/lfs-files/{sha}`
- [ ] `GET /api/datasets/{namespace}/{repo}/notebook/{rev}/{path}`
- [ ] `POST /api/datasets/{namespace}/{repo}/paths-info/{rev}`
- [ ] `POST /api/datasets/{namespace}/{repo}/preupload/{rev}`
- [x] `GET /api/datasets/{namespace}/{repo}/refs` → `datasetRefs()`
- [x] `POST /api/datasets/{namespace}/{repo}/resource-group` → `setDatasetResourceGroup()`
- [x] `POST /api/datasets/{namespace}/{repo}/scan` → `scanDataset()`
- [ ] `PUT /api/datasets/{namespace}/{repo}/settings` (implemented via `updateRepoSettings()`)
- [x] `POST /api/datasets/{namespace}/{repo}/super-squash/{rev}` → `superSquashDataset()`
- [x] `POST /api/datasets/{namespace}/{repo}/tag/{rev}` → `createDatasetTag()`
- [x] `GET /api/datasets/{namespace}/{repo}/tree/{rev}/{path}` → `datasetTree()`
- [x] `GET /api/datasets/{namespace}/{repo}/treesize/{rev}/{path}` → `datasetTreeSize()`
- [x] `POST /api/datasets/{namespace}/{repo}/user-access-request/cancel` → `cancelDatasetAccessRequest()`
- [x] `POST /api/datasets/{namespace}/{repo}/user-access-request/grant` → `grantDatasetAccess()`
- [x] `POST /api/datasets/{namespace}/{repo}/user-access-request/handle` → `handleDatasetAccessRequest()`
- [x] `GET /api/datasets/{namespace}/{repo}/user-access-request/{status}` → `listDatasetAccessRequests()`
- [ ] `GET /api/datasets/{namespace}/{repo}/xet-read-token/{rev}`
- [ ] `GET /api/datasets/{namespace}/{repo}/xet-write-token/{rev}`
- [ ] `GET /datasets/{namespace}/{repo}/resolve/{rev}/{path}`
- [x] `POST /datasets/{namespace}/{repo}/ask-access` → `requestDatasetAccess()`
- [x] `GET /datasets/{namespace}/{repo}/user-access-report` → `getDatasetUserAccessReport()`

### Models
- [x] `GET /api/models` → `listModels()`
- [x] `GET /api/models/{namespace}/{repo}` → `getModel()`
- [x] `GET /api/models-tags-by-type` → `getModelTags()`
- [ ] `GET /api/models/{namespace}/{repo}/branch/{rev}`
- [ ] `POST /api/models/{namespace}/{repo}/commit/{rev}`
- [x] `GET /api/models/{namespace}/{repo}/commits/{rev}` → `modelCommits()`
- [x] `GET /api/models/{namespace}/{repo}/compare/{compare}` → `compareModelRevisions()`
- [ ] `GET /api/models/{namespace}/{repo}/lfs-files`
- [ ] `POST /api/models/{namespace}/{repo}/lfs-files/batch`
- [ ] `GET /api/models/{namespace}/{repo}/lfs-files/{sha}`
- [ ] `GET /api/models/{namespace}/{repo}/notebook/{rev}/{path}`
- [ ] `POST /api/models/{namespace}/{repo}/paths-info/{rev}`
- [ ] `POST /api/models/{namespace}/{repo}/preupload/{rev}`
- [x] `GET /api/models/{namespace}/{repo}/refs` → `modelRefs()`
- [x] `POST /api/models/{namespace}/{repo}/resource-group` → `setModelResourceGroup()`
- [x] `POST /api/models/{namespace}/{repo}/scan` → `scanModel()`
- [ ] `PUT /api/models/{namespace}/{repo}/settings` (implemented via `updateRepoSettings()`)
- [x] `POST /api/models/{namespace}/{repo}/super-squash/{rev}` → `superSquashModel()`
- [x] `POST /api/models/{namespace}/{repo}/tag/{rev}` → `createModelTag()`
- [x] `GET /api/models/{namespace}/{repo}/tree/{rev}/{path}` → `modelTree()`
- [x] `GET /api/models/{namespace}/{repo}/treesize/{rev}/{path}` → `modelTreeSize()`
- [x] `POST /api/models/{namespace}/{repo}/user-access-request/cancel` → `cancelModelAccessRequest()`
- [x] `POST /api/models/{namespace}/{repo}/user-access-request/grant` → `grantModelAccess()`
- [x] `POST /api/models/{namespace}/{repo}/user-access-request/handle` → `handleModelAccessRequest()`
- [x] `GET /api/models/{namespace}/{repo}/user-access-request/{status}` → `listModelAccessRequests()`
- [ ] `GET /api/models/{namespace}/{repo}/xet-read-token/{rev}`
- [ ] `GET /api/models/{namespace}/{repo}/xet-write-token/{rev}`
- [ ] `GET /{namespace}/{repo}/resolve/{rev}/{path}`
- [x] `POST /{namespace}/{repo}/ask-access` → `requestModelAccess()`
- [x] `GET /{namespace}/{repo}/user-access-report` → `getModelUserAccessReport()`

### Organizations
- [x] `GET /api/organizations` → `listOrganizations()`
- [x] `GET /api/organizations/{name}` → `getOrganization()`
- [x] `GET /api/organizations/{name}/members` → `listOrganizationMembers()`
- [ ] `POST /api/organizations/{name}/audit-log/export`
- [ ] `POST /api/organizations/{name}/avatar`
- [x] `GET /api/organizations/{name}/billing/usage` → `getOrganizationBillingUsage()`
- [x] `GET /api/organizations/{name}/billing/usage/live` → `getOrganizationBillingUsageLive()`
- [x] `POST /api/organizations/{name}/resource-groups` → `createOrganizationResourceGroup()`
- [ ] `GET /api/organizations/{name}/scim/v2/Groups`
- [ ] `GET /api/organizations/{name}/scim/v2/Groups/{groupId}`
- [ ] `GET /api/organizations/{name}/scim/v2/Users`
- [ ] `GET /api/organizations/{name}/scim/v2/Users/{userId}`
- [ ] `POST /api/organizations/{name}/socials`

### Papers
- [x] `GET /api/papers` → `listPapers()` (note: API spec shows `/api/papers/search`)
- [x] `GET /api/papers/{paperId}` → `getPaper()`
- [x] `GET /api/daily_papers` → `listDailyPapers()`
- [ ] `POST /api/papers/{paperId}/comment`
- [ ] `POST /api/papers/{paperId}/comment/{commentId}/reply`

### Repository Management
- [x] `POST /api/repos/create` → `createRepo()`
- [x] `POST /api/repos/move` → `moveRepo()`

### Spaces
- [x] `GET /api/spaces` → `listSpaces()`
- [x] `GET /api/spaces/{namespace}/{repo}` → `getSpace()`
- [x] `GET /api/spaces/{namespace}/{repo}/runtime` → `spaceRuntime()`
- [x] `POST /api/spaces/{namespace}/{repo}/sleeptime` → `sleepSpace()`
- [x] `POST /api/spaces/{namespace}/{repo}/restart` → `restartSpace()`
- [ ] `GET /api/spaces/{namespace}/{repo}/branch/{rev}`
- [ ] `POST /api/spaces/{namespace}/{repo}/commit/{rev}`
- [x] `GET /api/spaces/{namespace}/{repo}/commits/{rev}` → `spaceCommits()`
- [x] `GET /api/spaces/{namespace}/{repo}/compare/{compare}` → `compareSpaceRevisions()`
- [x] `GET /api/spaces/{namespace}/{repo}/events` → `streamSpaceEvents()`
- [ ] `GET /api/spaces/{namespace}/{repo}/lfs-files`
- [ ] `POST /api/spaces/{namespace}/{repo}/lfs-files/batch`
- [ ] `GET /api/spaces/{namespace}/{repo}/lfs-files/{sha}`
- [x] `GET /api/spaces/{namespace}/{repo}/logs/{logType}` → `streamSpaceLogs()`
- [x] `GET /api/spaces/{namespace}/{repo}/metrics` → `streamSpaceMetrics()`
- [ ] `GET /api/spaces/{namespace}/{repo}/notebook/{rev}/{path}`
- [ ] `POST /api/spaces/{namespace}/{repo}/paths-info/{rev}`
- [ ] `POST /api/spaces/{namespace}/{repo}/preupload/{rev}`
- [x] `GET /api/spaces/{namespace}/{repo}/refs` → `spaceRefs()`
- [x] `POST /api/spaces/{namespace}/{repo}/resource-group` → `setSpaceResourceGroup()`
- [x] `POST /api/spaces/{namespace}/{repo}/scan` → `scanSpace()`
- [ ] `GET /api/spaces/{namespace}/{repo}/secrets`
- [x] `POST /api/spaces/{namespace}/{repo}/secrets` → `upsertSpaceSecret()`
- [ ] `PUT /api/spaces/{namespace}/{repo}/settings` (implemented via `updateRepoSettings()`)
- [x] `POST /api/spaces/{namespace}/{repo}/super-squash/{rev}` → `superSquashSpace()`
- [x] `POST /api/spaces/{namespace}/{repo}/tag/{rev}` → `createSpaceTag()`
- [x] `GET /api/spaces/{namespace}/{repo}/tree/{rev}/{path}` → `spaceTree()`
- [x] `GET /api/spaces/{namespace}/{repo}/treesize/{rev}/{path}` → `spaceTreeSize()`
- [ ] `GET /api/spaces/{namespace}/{repo}/variables`
- [x] `POST /api/spaces/{namespace}/{repo}/variables` → `upsertSpaceVariable()`
- [ ] `GET /api/spaces/{namespace}/{repo}/xet-read-token/{rev}`
- [ ] `GET /api/spaces/{namespace}/{repo}/xet-write-token/{rev}`
- [ ] `GET /spaces/{namespace}/{repo}/resolve/{rev}/{path}`

### User
- [x] `GET /api/whoami-v2` → `whoami()`
- [x] `GET /oauth/userinfo` → `getOAuthUserInfo()`
- [ ] `GET /api/users/{username}/billing/usage/live`
- [ ] `POST /api/users/{username}/socials`

### Repository Settings
- [x] `PUT /api/{repoType}/{namespace}/{repo}/settings` → `updateRepoSettings()`

### Discussions
- [x] `GET /api/{repoType}/{namespace}/{repo}/discussions` → `listDiscussions()`
- [x] `GET /api/{repoType}/{namespace}/{repo}/discussions/{num}` → `getDiscussion()`
- [x] `POST /api/{repoType}/{namespace}/{repo}/discussions/{num}/comment` → `addCommentToDiscussion()`
- [x] `POST /api/{repoType}/{namespace}/{repo}/discussions/{num}/merge` → `mergeDiscussion()`
- [x] `POST /api/{repoType}/{namespace}/{repo}/discussions/{num}/pin` → `pinDiscussion()`
- [x] `PATCH /api/{repoType}/{namespace}/{repo}/discussions/{num}/status` → `updateDiscussionStatus()`
- [x] `PATCH /api/{repoType}/{namespace}/{repo}/discussions/{num}/title` → `updateDiscussionTitle()`
- [x] `POST /api/discussions/mark-as-read` → `markDiscussionsAsRead()`

##### Blog Comments
- [ ] `POST /api/blog/{namespace}/{slug}/comment`
- [ ] `POST /api/blog/{namespace}/{slug}/comment/{commentId}/reply`
- [ ] `POST /api/blog/{slug}/comment`
- [ ] `POST /api/blog/{slug}/comment/{commentId}/reply`

##### Documentation
- [ ] `GET /api/docs/search`

##### Jobs
- [ ] `GET /api/jobs/{namespace}`
- [ ] `GET /api/jobs/{namespace}/{jobId}`
- [ ] `POST /api/jobs/{namespace}/{jobId}/cancel`
- [ ] `GET /api/jobs/{namespace}/{jobId}/events`
- [ ] `GET /api/jobs/{namespace}/{jobId}/logs`
- [ ] `GET /api/jobs/{namespace}/{jobId}/metrics`

##### Notifications
- [ ] `GET /api/notifications`

##### Posts
- [ ] `GET /api/posts/{username}/{postSlug}`
- [ ] `POST /api/posts/{username}/{postSlug}/comment`
- [ ] `POST /api/posts/{username}/{postSlug}/comment/{commentId}/reply`

##### Resolve Cache
- [ ] `GET /api/resolve-cache/datasets/{namespace}/{repo}/{rev}/{path}`
- [ ] `GET /api/resolve-cache/models/{namespace}/{repo}/{rev}/{path}`
- [ ] `GET /api/resolve-cache/spaces/{namespace}/{repo}/{rev}/{path}`

##### Scheduled Jobs
- [ ] `GET /api/scheduled-jobs/{namespace}`
- [ ] `GET /api/scheduled-jobs/{namespace}/{scheduledJobId}`
- [ ] `POST /api/scheduled-jobs/{namespace}/{scheduledJobId}/resume`
- [ ] `POST /api/scheduled-jobs/{namespace}/{scheduledJobId}/suspend`

##### Settings & User Management
- [ ] `GET /api/settings/billing/usage`
- [ ] `GET /api/settings/billing/usage/jobs`
- [ ] `GET /api/settings/billing/usage/live`
- [ ] `GET /api/settings/mcp`
- [ ] `GET /api/settings/notifications`
- [ ] `GET /api/settings/watch`
- [ ] `GET /api/settings/webhooks`
- [ ] `POST /api/settings/webhooks`
- [ ] `GET /api/settings/webhooks/{webhookId}`
- [ ] `DELETE /api/settings/webhooks/{webhookId}`
- [ ] `POST /api/settings/webhooks/{webhookId}/replay/{logId}`
- [ ] `POST /api/settings/webhooks/{webhookId}/{action}`

##### SQL Console
- [ ] `GET /api/{repoType}/{namespace}/{repo}/sql-console/embed`
- [ ] `GET /api/{repoType}/{namespace}/{repo}/sql-console/embed/{id}`

</details>

---

### Inference Providers API

The Inference Providers API allows you to run AI tasks using various models and providers. 
It automatically handles authentication and routing to the best provider for your needs.

#### Creating an Inference Client

```swift
import HuggingFace

// Create a client (uses auto-detected credentials from environment)
let client = InferenceClient.default
```

#### Chat Completion

Generate conversational responses using language models:

```swift
let messages: [ChatCompletion.Message] = [
    .system("You are a helpful assistant."),
    .user("What is the capital of France?")
]

let response = try await client.chatCompletion(
    model: "meta-llama/Llama-3.3-70B-Instruct",
    messages: messages,
    provider: .groq,
    temperature: 0.7,
    maxTokens: 1000
)

print(response.choices.first?.message.content ?? "")

// Streaming chat completion
for try await chunk in client.chatCompletionStream(
    model: "meta-llama/Llama-3.3-70B-Instruct",
    messages: messages,
    provider: .groq
) {
    if let content = chunk.choices.first?.delta.content {
        print(content, terminator: "")
    }
}
```

Vision-Language Models:

```swift
let messages: [ChatCompletion.Message] = [
    .init(role: .user, content: .mixed([
        .text("What's in this image?"),
        .image(url: "https://example.com/image.jpg", detail: .auto)
    ]))
]

let response = try await client.chatCompletion(
    model: "meta-llama/Llama-3.2-11B-Vision-Instruct",
    messages: messages,
    provider: .hyperbolic
)
```

#### Feature Extraction

Extract embeddings from text for similarity search and semantic analysis:

```swift
let response = try await client.featureExtraction(
    model: "sentence-transformers/all-MiniLM-L6-v2",
    inputs: [
        "The quick brown fox jumps over the lazy dog",
        "A fast auburn fox leaps above an idle canine"
    ],
    provider: .hfInference,
    normalize: true
)

for embedding in response.embeddings {
    print("Embedding dimension: \(embedding.count)")
}
```

#### Text-to-Image

Generate images from text prompts:

```swift
let response = try await client.textToImage(
    model: "black-forest-labs/FLUX.1-schnell",
    prompt: "A serene Japanese garden with cherry blossoms",
    provider: .hfInference,
    width: 1024,
    height: 1024,
    numImages: 1,
    guidanceScale: 7.5,
    numInferenceSteps: 50,
    seed: 42
)

// Save the generated image
try response.image.write(to: URL(fileURLWithPath: "generated.png"))
```

Advanced options:

```swift
let response = try await client.textToImage(
    model: "stabilityai/stable-diffusion-xl-base-1.0",
    prompt: "A futuristic cityscape at sunset",
    provider: .replicate,
    negativePrompt: "blurry, low quality, distorted",
    width: 1024,
    height: 1024,
    guidanceScale: 8.0
)
```

#### Text-to-Video

Generate videos from text descriptions:

```swift
let response = try await client.textToVideo(
    model: "stabilityai/stable-video-diffusion-img2vid-xt",
    prompt: "A cat playing with a ball of yarn",
    provider: .hfInference,
    width: 1024,
    height: 576,
    numFrames: 24,
    frameRate: 8,
    guidanceScale: 7.5,
    duration: 3.0
)

// Save the generated video
try response.video.write(to: URL(fileURLWithPath: "generated.mp4"))
```

#### Speech-to-Text

Transcribe audio to text:

```swift
// Base64-encode your audio file
let audioData = try Data(contentsOf: audioFileURL)
let audioBase64 = audioData.base64EncodedString()

let response = try await client.speechToText(
    model: "openai/whisper-large-v3",
    audio: audioBase64,
    provider: .hfInference,
    language: "en",
    task: .transcribe
)

print("Transcription: \(response.text)")
```

## Contributing

This is a community project and we welcome contributions.
Please check out
[Issues tagged with `good first issue`][good-first-issues]
if you are looking for a place to start!

Please ensure your code passes the build and test suite
before submitting a pull request.
You can run the tests with `swift test`.

[good-first-issues]: https://github.com/huggingface/swift-huggingface/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22

## License

[Apache 2](LICENSE).
