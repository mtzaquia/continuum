import SwiftUI

struct ContentView: View {
    @State private var catalog = DemoCatalog()
    @State private var shelf: Shelf = .home

    var body: some View {
        NavigationStack {
            ScrollView {
                LazyVStack(spacing: 16) {
                    HeroCard(catalog: catalog)
                    pipelineCard
                    activityCard
                    snapshotCard
                    recommendationCard
                }
                .padding(.horizontal, 16)
                .padding(.bottom, 32)
            }
            .background(Color(uiColor: .systemGroupedBackground))
            .navigationBarTitleDisplayMode(.inline)
            .toolbarBackground(.hidden, for: .navigationBar)
            .task { await catalog.loadCatalog() }
        }
        .tint(.indigo)
    }

    private var pipelineCard: some View {
        DemoCard {
            CardHeading(
                number: "01",
                title: "Run the load pipeline",
                detail: "The same action crosses cache, remote, and persistence."
            )

            HStack(spacing: 6) {
                FlowNode(
                    icon: "memorychip",
                    title: "Cache",
                    detail: "Immediate",
                    tint: .orange,
                    isActive: catalog.products.isLoaded
                )
                FlowConnector()
                FlowNode(
                    icon: "network",
                    title: "Remote",
                    detail: "Required",
                    tint: .indigo,
                    isActive: catalog.products.isLoading
                )
                FlowConnector()
                FlowNode(
                    icon: "internaldrive",
                    title: "Persist",
                    detail: "Before memory",
                    tint: .teal,
                    isActive: catalog.products.isLoaded && !catalog.products.isLoading
                )
            }

            Button {
                Task { await catalog.loadCatalog() }
            } label: {
                HStack {
                    Image(systemName: catalog.products.isLoading ? "arrow.trianglehead.2.clockwise.rotate.90" : "play.fill")
                    Text(catalog.products.isLoading ? "Running pipeline…" : "Run cached → remote")
                    Spacer()
                    Image(systemName: "chevron.right")
                        .font(.caption.weight(.bold))
                }
                .frame(maxWidth: .infinity)
            }
            .buttonStyle(PrimaryActionButtonStyle())
            .disabled(catalog.products.isLoading)
            .accessibilityIdentifier("catalog.refresh")

            HStack(spacing: 10) {
                CompactActionButton(
                    title: catalog.isFailureQueued ? "Failure queued" : "Queue failure",
                    icon: "bolt.trianglebadge.exclamationmark",
                    tint: .pink
                ) {
                    Task { await catalog.failNextCatalogRequest() }
                }
                .accessibilityIdentifier("catalog.fail-next")

                CompactActionButton(
                    title: "Store item",
                    icon: "plus",
                    tint: .teal
                ) {
                    Task { await catalog.addProduct() }
                }
                .accessibilityIdentifier("catalog.store")

                CompactActionButton(
                    title: "Reset",
                    icon: "arrow.counterclockwise",
                    tint: .orange
                ) {
                    catalog.resetCatalog()
                }
                .accessibilityIdentifier("catalog.reset")
            }
        }
    }

    private var activityCard: some View {
        DemoCard {
            CardHeading(
                number: "LIVE",
                title: "What just happened?",
                detail: "A plain-language trace of the latest operations."
            )

            ForEach(Array(catalog.activity.prefix(4).enumerated()), id: \.element.id) { index, event in
                ActivityRow(event: event, isLast: index == min(catalog.activity.count, 4) - 1)
            }
        }
    }

    private var snapshotCard: some View {
        DemoCard {
            CardHeading(
                number: "02",
                title: "Inspect the atomic snapshot",
                detail: "Values change together; pagination and errors have separate state."
            )

            HStack(spacing: 8) {
                StatePill(
                    icon: catalog.products.isLoaded ? "checkmark.circle.fill" : "circle.dashed",
                    text: catalog.products.isLoaded ? "Established" : "Untouched",
                    tint: catalog.products.isLoaded ? .green : .secondary
                )
                StatePill(
                    icon: "square.stack.3d.up.fill",
                    text: "\(catalog.products.count) values",
                    tint: .indigo
                )
                if catalog.products.hasNextPage {
                    StatePill(icon: "arrow.down", text: "More", tint: .orange)
                }
            }

            if let error = catalog.products.error {
                ErrorBanner(
                    title: "Snapshot preserved",
                    message: error.localizedDescription
                )
                .accessibilityIdentifier("catalog.error")
            }

            if catalog.products.values.isEmpty {
                EmptySnapshotView()
            } else {
                VStack(spacing: 0) {
                    ForEach(Array(catalog.products.values.enumerated()), id: \.element.id) { index, product in
                        ProductRow(product: product) {
                            Task { await catalog.remove(product) }
                        }
                        if index < catalog.products.values.count - 1 {
                            Divider().padding(.leading, 50)
                        }
                    }
                }
            }

            if catalog.products.isLoadingNextPage {
                HStack(spacing: 10) {
                    ProgressView()
                    Text("Following the next cursor…")
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity)
                .padding()
                .accessibilityIdentifier("catalog.loading-next")
            } else if catalog.products.hasNextPage {
                Button {
                    Task { await catalog.loadNextPage() }
                } label: {
                    Label("Merge next page", systemImage: "square.stack.3d.down.forward")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(SecondaryActionButtonStyle(tint: .indigo))
                .accessibilityIdentifier("catalog.load-next")
            }

            if let error = catalog.products.nextPageError {
                ErrorBanner(
                    title: "Page failed independently",
                    message: error.localizedDescription
                )
                .accessibilityIdentifier("catalog.next-error")
            }
        }
    }

    private var recommendationCard: some View {
        let partition = catalog.recommendations[shelf]

        return DemoCard {
            CardHeading(
                number: "03",
                title: "Compare independent partitions",
                detail: "Each shelf keeps its own values, loading state, and failures."
            )

            Picker("Shelf", selection: $shelf) {
                ForEach(Shelf.allCases) { shelf in
                    Text(shelf.title).tag(shelf)
                }
            }
            .pickerStyle(.segmented)
            .onChange(of: shelf) { _, shelf in
                guard catalog.recommendations[shelf].isLoaded == false else { return }
                Task { await catalog.loadRecommendations(for: shelf) }
            }

            HStack {
                StatePill(
                    icon: partition.isLoaded ? "checkmark.circle.fill" : "circle.dashed",
                    text: partition.isLoaded ? "\(shelf.title) established" : "\(shelf.title) untouched",
                    tint: partition.isLoaded ? .green : .secondary
                )
                Spacer()
                Text("\(partition.count) values")
                    .font(.caption.monospacedDigit())
                    .foregroundStyle(.secondary)
            }

            if partition.isLoading {
                HStack(spacing: 10) {
                    ProgressView()
                    Text("Loading only the \(shelf.title.lowercased()) shelf…")
                }
                .frame(maxWidth: .infinity)
                .padding()
            } else if partition.isLoaded == false {
                Button {
                    Task { await catalog.loadRecommendations(for: shelf) }
                } label: {
                    Label("Load \(shelf.title) partition", systemImage: "square.split.2x1")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(SecondaryActionButtonStyle(tint: .teal))
                .accessibilityIdentifier("recommendations.load")
            } else {
                ForEach(partition.values) { product in
                    ProductRow(product: product, delete: nil)
                }
            }

            Text("Tip: load Home, switch to Office, then switch back. Home stays established.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }
}

private struct HeroCard: View {
    let catalog: DemoCatalog

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            HStack {
                Label("INTERACTIVE SAMPLE", systemImage: "sparkles")
                    .font(.caption2.weight(.bold))
                    .tracking(1.2)
                Spacer()
                Text("TRACE ON")
                    .font(.caption2.weight(.bold).monospaced())
                    .padding(.horizontal, 9)
                    .padding(.vertical, 5)
                    .background(.white.opacity(0.16), in: Capsule())
            }

            VStack(alignment: .leading, spacing: 7) {
                Text("Continuum Lab")
                    .font(.largeTitle.bold())
                Text("See typed repository state move from source to snapshot—one deliberate step at a time.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.82))
                    .fixedSize(horizontal: false, vertical: true)
            }

            HStack(spacing: 8) {
                Image(systemName: heroIcon)
                Text(heroStatus)
                    .font(.subheadline.weight(.semibold))
                Spacer()
                if catalog.products.isLoading {
                    ProgressView().tint(.white)
                }
            }
            .padding(12)
            .background(.black.opacity(0.15), in: RoundedRectangle(cornerRadius: 14))
        }
        .foregroundStyle(.white)
        .padding(20)
        .background(
            LinearGradient(
                colors: [.indigo, Color(red: 0.42, green: 0.23, blue: 0.78), .purple],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            ),
            in: RoundedRectangle(cornerRadius: 28, style: .continuous)
        )
        .overlay(alignment: .topTrailing) {
            Circle()
                .fill(.white.opacity(0.08))
                .frame(width: 150, height: 150)
                .offset(x: 45, y: -60)
        }
    }

    private var heroIcon: String {
        if catalog.products.isLoading { return "arrow.trianglehead.2.clockwise.rotate.90" }
        if catalog.products.error != nil { return "exclamationmark.triangle.fill" }
        if catalog.products.isLoaded { return "checkmark.seal.fill" }
        return "circle.dashed"
    }

    private var heroStatus: String {
        if catalog.products.isLoading { return "Resolving the latest snapshot" }
        if catalog.products.error != nil { return "Cached snapshot preserved after failure" }
        if catalog.products.isLoaded { return "\(catalog.products.count) values established in memory" }
        return "No snapshot established yet"
    }
}

private struct DemoCard<Content: View>: View {
    @ViewBuilder let content: Content

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            content
        }
        .padding(18)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            Color(uiColor: .secondarySystemGroupedBackground),
            in: RoundedRectangle(cornerRadius: 24, style: .continuous)
        )
    }
}

private struct CardHeading: View {
    let number: String
    let title: String
    let detail: String

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Text(number)
                .font(.caption.bold().monospaced())
                .foregroundStyle(.indigo)
                .frame(minWidth: 34)
                .padding(.vertical, 7)
                .background(.indigo.opacity(0.1), in: RoundedRectangle(cornerRadius: 9))

            VStack(alignment: .leading, spacing: 3) {
                Text(title)
                    .font(.headline)
                Text(detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }
}

private struct FlowNode: View {
    let icon: String
    let title: String
    let detail: String
    let tint: Color
    let isActive: Bool

    var body: some View {
        VStack(spacing: 7) {
            Image(systemName: icon)
                .font(.headline)
                .foregroundStyle(tint)
                .frame(width: 38, height: 38)
                .background(tint.opacity(isActive ? 0.18 : 0.08), in: Circle())
            Text(title)
                .font(.caption.weight(.semibold))
            Text(detail)
                .font(.caption2)
                .foregroundStyle(.secondary)
                .lineLimit(1)
        }
        .frame(maxWidth: .infinity)
    }
}

private struct FlowConnector: View {
    var body: some View {
        Image(systemName: "chevron.right")
            .font(.caption2.bold())
            .foregroundStyle(.tertiary)
    }
}

private struct ActivityRow: View {
    let event: DemoActivity
    let isLast: Bool

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            VStack(spacing: 4) {
                Image(systemName: icon)
                    .font(.caption.bold())
                    .foregroundStyle(tint)
                    .frame(width: 28, height: 28)
                    .background(tint.opacity(0.12), in: Circle())
                if !isLast {
                    Rectangle()
                        .fill(Color.secondary.opacity(0.18))
                        .frame(width: 2, height: 24)
                }
            }

            VStack(alignment: .leading, spacing: 3) {
                HStack {
                    Text(event.title)
                        .font(.subheadline.weight(.semibold))
                    Spacer()
                    Text(event.date, style: .time)
                        .font(.caption2.monospacedDigit())
                        .foregroundStyle(.tertiary)
                }
                Text(event.detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
            .padding(.top, 3)
        }
    }

    private var icon: String {
        switch event.kind {
        case .request: "arrow.right"
        case .cache: "memorychip"
        case .remote: "network"
        case .mutation: "arrow.triangle.2.circlepath"
        case .success: "checkmark"
        case .failure: "exclamationmark"
        }
    }

    private var tint: Color {
        switch event.kind {
        case .request: .indigo
        case .cache: .orange
        case .remote: .blue
        case .mutation: .teal
        case .success: .green
        case .failure: .pink
        }
    }
}

private struct StatePill: View {
    let icon: String
    let text: String
    let tint: Color

    var body: some View {
        Label(text, systemImage: icon)
            .font(.caption2.weight(.semibold))
            .foregroundStyle(tint)
            .padding(.horizontal, 9)
            .padding(.vertical, 6)
            .background(tint.opacity(0.1), in: Capsule())
    }
}

private struct CompactActionButton: View {
    let title: String
    let icon: String
    let tint: Color
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            VStack(spacing: 6) {
                Image(systemName: icon)
                    .font(.headline)
                Text(title)
                    .font(.caption2.weight(.semibold))
                    .lineLimit(1)
                    .minimumScaleFactor(0.75)
            }
            .foregroundStyle(tint)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 11)
            .background(tint.opacity(0.09), in: RoundedRectangle(cornerRadius: 13))
        }
        .buttonStyle(.plain)
    }
}

private struct ProductRow: View {
    let product: Product
    let delete: (() -> Void)?

    var body: some View {
        HStack(spacing: 12) {
            Text(product.name.prefix(1))
                .font(.headline)
                .foregroundStyle(.indigo)
                .frame(width: 38, height: 38)
                .background(.indigo.opacity(0.1), in: RoundedRectangle(cornerRadius: 11))

            VStack(alignment: .leading, spacing: 3) {
                Text(product.name)
                    .font(.subheadline.weight(.semibold))
                Text(product.detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("product.\(product.id)")
            }

            Spacer(minLength: 6)

            VStack(alignment: .trailing, spacing: 5) {
                Text("$\(product.price)")
                    .font(.subheadline.weight(.semibold).monospacedDigit())
                if let delete {
                    Button(role: .destructive, action: delete) {
                        Image(systemName: "trash")
                            .font(.caption)
                    }
                    .accessibilityLabel("Remove \(product.name)")
                }
            }
        }
        .padding(.vertical, 10)
    }
}

private struct ErrorBanner: View {
    let title: String
    let message: String

    var body: some View {
        HStack(alignment: .top, spacing: 10) {
            Image(systemName: "exclamationmark.triangle.fill")
            VStack(alignment: .leading, spacing: 2) {
                Text(title).font(.subheadline.weight(.semibold))
                Text(message).font(.caption)
            }
        }
        .foregroundStyle(.pink)
        .padding(12)
        .background(.pink.opacity(0.09), in: RoundedRectangle(cornerRadius: 14))
    }
}

private struct EmptySnapshotView: View {
    var body: some View {
        VStack(spacing: 8) {
            Image(systemName: "tray")
                .font(.title2)
                .foregroundStyle(.secondary)
            Text("No snapshot in memory")
                .font(.subheadline.weight(.semibold))
            Text("Run the pipeline to restore cache and refresh remote.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 20)
    }
}

private struct PrimaryActionButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.subheadline.weight(.semibold))
            .foregroundStyle(.white)
            .padding(14)
            .background(
                configuration.isPressed ? Color.indigo.opacity(0.75) : .indigo,
                in: RoundedRectangle(cornerRadius: 14)
            )
    }
}

private struct SecondaryActionButtonStyle: ButtonStyle {
    let tint: Color

    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.subheadline.weight(.semibold))
            .foregroundStyle(tint)
            .padding(13)
            .background(
                tint.opacity(configuration.isPressed ? 0.17 : 0.1),
                in: RoundedRectangle(cornerRadius: 14)
            )
    }
}

#Preview {
    ContentView()
}
