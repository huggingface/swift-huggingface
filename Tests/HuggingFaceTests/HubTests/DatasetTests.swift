import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Dataset Tests", .serialized)
    struct DatasetTests {
        private func createClient() -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
            let session = URLSession(configuration: configuration)

            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0",
                cache: nil
            )
        }

        @Test(
            "List datasets with no parameters",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets?limit=2",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        [
                            {
                                "id": "datasets/squad",
                                "author": "datasets",
                                "downloads": 500000,
                                "likes": 250
                            },
                            {
                                "id": "stanfordnlp/imdb",
                                "author": "stanfordnlp",
                                "downloads": 300000,
                                "likes": 150
                            }
                        ]
                        """
                    }
                ]
            )
        )
        func testListDatasets() async throws {
            let client = createClient()
            let result = try await client.listDatasets(limit: 2)

            #expect(result.items.count == 2)
            #expect(result.items[0].id == "datasets/squad")
            #expect(result.items[0].author == "datasets")
            #expect(result.items[1].id == "stanfordnlp/imdb")
        }

        @Test(
            "List datasets with search parameter",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets?search=squad",
                        200,
                        ["Content-Type": "application/json"],
                        {
                            """
                            [
                                {
                                    "id": "datasets/squad",
                                    "author": "datasets",
                                    "downloads": 500000,
                                    "likes": 250
                                }
                            ]
                            """
                        }
                    )
                ]
            )
        )
        func testListDatasetsWithSearch() async throws {
            let client = createClient()
            let result = try await client.listDatasets(search: "squad")

            #expect(result.items.count == 1)
            #expect(result.items[0].id == "datasets/squad")
        }

        @Test(
            "Get specific dataset",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets/_/squad",
                        200,
                        ["Content-Type": "application/json"],
                        {
                            """
                            {
                                "id": "_/squad",
                                "author": "datasets",
                                "downloads": 500000,
                                "likes": 250,
                                "tags": ["question-answering"]
                            }
                            """
                        }
                    )
                ]
            )
        )
        func testGetDataset() async throws {
            let client = createClient()
            let repoID: Repo.ID = "_/squad"
            let dataset = try await client.getDataset(repoID)

            #expect(dataset.id == "_/squad")
            #expect(dataset.author == "datasets")
            #expect(dataset.downloads == 500000)
        }

        @Test(
            "Get dataset with namespace",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets/huggingface/squad",
                        200,
                        ["Content-Type": "application/json"],
                        {
                            """
                            {
                                "id": "huggingface/squad",
                                "author": "huggingface",
                                "downloads": 500000,
                                "likes": 250
                            }
                            """
                        }
                    )
                ]
            )
        )
        func testGetDatasetWithNamespace() async throws {
            let client = createClient()
            let repoID: Repo.ID = "huggingface/squad"
            let dataset = try await client.getDataset(repoID)

            #expect(dataset.id == "huggingface/squad")
            #expect(dataset.author == "huggingface")
        }

        @Test(
            "Get dataset tags",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets-tags-by-type",
                        200,
                        ["Content-Type": "application/json"],
                        {
                            """
                            {
                                "tags": {
                                    "task_categories": [
                                        {"id": "question-answering", "label": "Question Answering"},
                                        {"id": "text-classification", "label": "Text Classification"}
                                    ],
                                    "languages": [
                                        {"id": "en", "label": "English"},
                                        {"id": "fr", "label": "French"}
                                    ]
                                }
                            }
                            """
                        }
                    )
                ]
            )
        )
        func testGetDatasetTags() async throws {
            let client = createClient()
            let tags = try await client.getDatasetTags()

            #expect(tags["task_categories"]?.count == 2)
            #expect(tags["languages"]?.count == 2)
        }

        @Test(
            "List parquet files",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets/_/squad/parquet",
                        200,
                        ["Content-Type": "application/json"],
                        {
                            """
                            [
                                {
                                    "dataset": "squad",
                                    "config": "default",
                                    "split": "train",
                                    "url": "https://huggingface.co/datasets/squad/resolve/main/data/train.parquet",
                                    "filename": "train.parquet",
                                    "size": 1024000
                                },
                                {
                                    "dataset": "squad",
                                    "config": "default",
                                    "split": "validation",
                                    "url": "https://huggingface.co/datasets/squad/resolve/main/data/validation.parquet",
                                    "filename": "validation.parquet",
                                    "size": 204800
                                }
                            ]
                            """
                        }
                    )
                ]
            )
        )
        func testListParquetFiles() async throws {
            let client = createClient()
            let repoID: Repo.ID = "_/squad"
            let files = try await client.listParquetFiles(repoID)

            #expect(files.count == 2)
            #expect(files[0].split == "train")
            #expect(files[1].split == "validation")
        }

        @Test(
            "List parquet files with subset",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets/_/squad/parquet/plain_text",
                        200,
                        ["Content-Type": "application/json"],
                        {
                            """
                            [
                                {
                                    "dataset": "squad",
                                    "config": "plain_text",
                                    "split": "train",
                                    "url": "https://huggingface.co/datasets/squad/resolve/main/data/plain_text/train.parquet",
                                    "filename": "train.parquet",
                                    "size": 1024000
                                }
                            ]
                            """
                        }
                    )
                ]
            )
        )
        func testListParquetFilesWithSubset() async throws {
            let client = createClient()
            let repoID: Repo.ID = "_/squad"
            let files = try await client.listParquetFiles(repoID, subset: "plain_text")

            #expect(files.count == 1)
            #expect(files[0].config == "plain_text")
        }

        @Test(
            "List parquet files with subset and split",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets/_/squad/parquet/plain_text/train",
                        200,
                        ["Content-Type": "application/json"],
                        {
                            """
                            [
                                {
                                    "dataset": "squad",
                                    "config": "plain_text",
                                    "split": "train",
                                    "url": "https://huggingface.co/datasets/squad/resolve/main/data/plain_text/train.parquet",
                                    "filename": "train.parquet",
                                    "size": 1024000
                                }
                            ]
                            """
                        }
                    )
                ]
            )
        )
        func testListParquetFilesWithSubsetAndSplit() async throws {
            let client = createClient()
            let repoID: Repo.ID = "_/squad"
            let files = try await client.listParquetFiles(
                repoID,
                subset: "plain_text",
                split: "train"
            )

            #expect(files.count == 1)
            #expect(files[0].split == "train")
        }

        @Test(
            "Handle 404 error for dataset",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets/nonexistent/dataset",
                        404,
                        ["Content-Type": "application/json"],
                        {
                            """
                            {
                                "error": "Dataset not found"
                            }
                            """
                        }
                    )
                ]
            )
        )
        func testGetDatasetNotFound() async throws {
            let client = createClient()
            let repoID: Repo.ID = "nonexistent/dataset"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getDataset(repoID)
            }
        }
    }

#endif  // swift(>=6.1)
