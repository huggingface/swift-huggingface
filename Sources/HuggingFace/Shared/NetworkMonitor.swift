import Foundation

#if canImport(Network)
    import Network

    /// Monitors network connectivity to determine if offline mode should be used.
    final class NetworkMonitor: Sendable {
        private let monitor: NWPathMonitor
        private let queue: DispatchQueue

        /// The current network state.
        let state: NetworkStateActor = .init()

        /// Shared singleton instance.
        static let shared = NetworkMonitor()

        init() {
            monitor = NWPathMonitor()
            queue = DispatchQueue(label: "HuggingFace.NetworkMonitor")
            startMonitoring()
        }

        func startMonitoring() {
            monitor.pathUpdateHandler = { [weak self] path in
                guard let self else { return }
                Task {
                    await self.state.update(path: path)
                }
            }
            monitor.start(queue: queue)
        }

        func stopMonitoring() {
            monitor.cancel()
        }

        deinit {
            stopMonitoring()
        }
    }

    /// Actor that safely holds network state.
    actor NetworkStateActor {
        /// Whether the network is connected. Assumes connected until updated.
        private(set) var isConnected: Bool = true

        /// Whether the connection is expensive (e.g., cellular).
        private(set) var isExpensive: Bool = false

        /// Whether the connection is constrained (e.g., Low Data Mode).
        private(set) var isConstrained: Bool = false

        func update(path: NWPath) {
            isConnected = path.status == .satisfied
            isExpensive = path.isExpensive
            isConstrained = path.isConstrained
        }

        /// Returns whether offline mode should be used based on network state.
        func shouldUseOfflineMode() -> Bool {
            // Allow CI to disable network monitoring for testing
            if ProcessInfo.processInfo.environment["CI_DISABLE_NETWORK_MONITOR"] == "1" {
                return false
            }
            return !isConnected
        }
    }

#else
    // Linux fallback - no Network framework available
    final class NetworkMonitor: Sendable {
        static let shared = NetworkMonitor()
        let state: NetworkStateActor = .init()

        init() {}
        func startMonitoring() {}
        func stopMonitoring() {}
    }

    actor NetworkStateActor {
        private(set) var isConnected: Bool = true
        private(set) var isExpensive: Bool = false
        private(set) var isConstrained: Bool = false

        func shouldUseOfflineMode() -> Bool {
            // On Linux, always assume online (no NWPathMonitor available)
            return false
        }
    }
#endif
