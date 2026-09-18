import Foundation

/// Namespace for Git operation types.
public enum Git {
    /// Represents an entry in a Git tree (file or directory).
    public struct TreeEntry: Hashable, Codable, Sendable {
        /// The path of the entry relative to the repository root.
        public let path: String

        /// The type of the entry (file or directory).
        public let type: EntryType

        /// The object ID (SHA) of the entry, if available.
        public let oid: String?

        /// The size of the entry in bytes, if available.
        public let size: Int?

        /// The Git LFS metadata, if available.
        public let lfs: LFSInfo?

        /// The LFS file size in bytes, or the entry size when LFS metadata is absent.
        public var effectiveSize: Int? {
            lfs?.size ?? size
        }

        /// Metadata for a file stored with Git Large File Storage.
        public struct LFSInfo: Hashable, Codable, Sendable {
            /// The SHA-256 object ID, if available.
            public let oid: String?

            /// The file size in bytes.
            public let size: Int

            /// The size of the Git LFS pointer in bytes, if available.
            public let pointerSize: Int?
        }

        /// The last commit information for this entry, if available.
        public let lastCommit: LastCommitInfo?

        /// The type of a tree entry.
        public enum EntryType: String, Hashable, CaseIterable, CustomStringConvertible, Codable, Sendable {
            /// A file entry.
            case file

            /// A directory entry.
            case directory

            public var description: String {
                return rawValue
            }
        }

        /// Information about the last commit that modified an entry.
        public struct LastCommitInfo: Hashable, Codable, Sendable {
            /// The commit ID (SHA).
            public let id: String

            /// The commit title.
            public let title: String

            /// The commit date.
            public let date: Date
        }
    }

    /// Represents a Git reference (branch or tag).
    public struct Ref: Hashable, Codable, Sendable {
        /// The name of the reference (e.g., "main", "v1.0.0").
        public let name: String

        /// The full reference path (e.g., "refs/heads/main").
        public let ref: String

        /// The target object ID (SHA).
        public let targetOid: String?

        private enum CodingKeys: String, CodingKey {
            case name
            case ref
            case targetOid = "targetOid"
        }
    }

    /// Represents a Git commit.
    public struct Commit: Identifiable, Hashable, Codable, Sendable {
        /// The commit ID (SHA).
        public let id: String

        /// The commit title (first line of the commit message).
        public let title: String

        /// The full commit message.
        public let message: String?

        /// The commit date.
        public let date: Date

        /// The authors of the commit.
        public let authors: [Author]

        /// The parent commit IDs.
        public let parents: [String]?
    }

    /// Represents a commit author.
    public struct Author: Hashable, Codable, Sendable {
        /// The author's name.
        public let name: String

        /// The author's email address.
        public let email: String?

        /// The timestamp of the commit.
        public let time: Date?

        /// The Hugging Face user information, if the author has an account.
        public let user: String?

        /// The avatar URL of the author, if available.
        public let avatarURL: String?
    }
}
