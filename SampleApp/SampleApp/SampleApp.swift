import Continuum
import SwiftUI

@main
struct SampleApp: App {
    init() {
        Continuum.debug = .trace
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}
