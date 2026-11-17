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
        ///
        /// Use ``file``, ``directory``, or ``unknown``,
        /// or create a type from its identifier
        /// to use one this package doesn't list yet.
        public struct EntryType: RawRepresentable, Hashable, CaseIterable, CustomStringConvertible, Codable,
            Sendable
        {
            /// The type's identifier, such as `"file"`.
            public let rawValue: String

            /// Creates a type from its identifier.
            public init(rawValue: String) {
                self.rawValue = rawValue
            }

            /// A file entry.
            public static let file = EntryType(rawValue: "file")

            /// A directory entry.
            public static let directory = EntryType(rawValue: "directory")

            /// An entry that the Hub can't classify.
            public static let unknown = EntryType(rawValue: "unknown")

            /// The types this package lists.
            public static let allCases: [EntryType] = [.file, .directory, .unknown]

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

        /// The SHA of the commit that the reference points to.
        public let targetCommit: String?

        /// The SHA of the commit that the reference points to.
        @available(*, deprecated, renamed: "targetCommit")
        public var targetOid: String? {
            targetCommit
        }

        private enum CodingKeys: String, CodingKey {
            case name
            case ref
            case targetCommit
            // Earlier versions encoded the commit with this key.
            case legacyTargetOid = "targetOid"
        }

        public init(from decoder: any Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            self.name = try container.decode(String.self, forKey: .name)
            self.ref = try container.decode(String.self, forKey: .ref)
            self.targetCommit =
                try container.decodeIfPresent(String.self, forKey: .targetCommit)
                ?? container.decodeIfPresent(String.self, forKey: .legacyTargetOid)
        }

        public func encode(to encoder: any Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(name, forKey: .name)
            try container.encode(ref, forKey: .ref)
            try container.encodeIfPresent(targetCommit, forKey: .targetCommit)
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
        ///
        /// The Hub doesn't send this value.
        public let parents: [String]?
    }

    /// Represents a commit author.
    public struct Author: Hashable, Codable, Sendable {
        /// The author's name.
        ///
        /// The Hub identifies commit authors by username,
        /// so for commits from the Hub this is the same as ``user``.
        public let name: String

        /// The author's email address.
        ///
        /// The Hub doesn't send this value.
        public let email: String?

        /// The timestamp of the commit.
        ///
        /// The Hub doesn't send this value.
        /// Use ``Commit/date`` instead.
        public let time: Date?

        /// The author's Hugging Face username.
        public let user: String?

        /// The avatar URL of the author, if available.
        public let avatarURL: String?

        private enum CodingKeys: String, CodingKey {
            case name
            case email
            case time
            case user
            case avatarURL = "avatar"
            // Earlier versions encoded the avatar URL with this key.
            case legacyAvatarURL = "avatarURL"
        }

        public init(from decoder: any Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            let user = try container.decodeIfPresent(String.self, forKey: .user)
            // The Hub sends only the username.
            // Earlier versions of this package encoded a name.
            if let name = try container.decodeIfPresent(String.self, forKey: .name) ?? user {
                self.name = name
            } else {
                throw DecodingError.keyNotFound(
                    CodingKeys.user,
                    DecodingError.Context(
                        codingPath: container.codingPath,
                        debugDescription: "An author needs a user or a name."
                    )
                )
            }
            self.user = user
            self.email = try container.decodeIfPresent(String.self, forKey: .email)
            self.time = try container.decodeIfPresent(Date.self, forKey: .time)
            self.avatarURL =
                try container.decodeIfPresent(String.self, forKey: .avatarURL)
                ?? container.decodeIfPresent(String.self, forKey: .legacyAvatarURL)
        }

        public func encode(to encoder: any Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(name, forKey: .name)
            try container.encodeIfPresent(email, forKey: .email)
            try container.encodeIfPresent(time, forKey: .time)
            try container.encodeIfPresent(user, forKey: .user)
            try container.encodeIfPresent(avatarURL, forKey: .avatarURL)
        }
    }
}
