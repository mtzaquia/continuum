import XCTest

final class SampleAppUITests: XCTestCase {
    @MainActor
    func testCatalogLoadsAndPaginates() {
        let app = XCUIApplication()
        app.launch()

        XCTAssertTrue(app.staticTexts["product.1"].waitForExistence(timeout: 3))
        app.buttons["catalog.load-next"].tap()
        XCTAssertTrue(app.staticTexts["product.5"].waitForExistence(timeout: 3))
    }

    @MainActor
    func testEachRecommendationPartitionCanLoad() {
        let app = XCUIApplication()
        app.launch()

        XCTAssertTrue(app.buttons["recommendations.load"].waitForExistence(timeout: 3))
        app.buttons["recommendations.load"].tap()
        XCTAssertTrue(app.staticTexts["product.20"].waitForExistence(timeout: 3))
    }
}
