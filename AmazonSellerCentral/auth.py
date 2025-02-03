import shutil
from datetime import datetime
from pathlib import Path
import pyotp
from playwright.sync_api import Page, Playwright, sync_playwright
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from helper.logging import logger
import yaml

# Constants
MARKET_PLACE_CONFIG_FILE_PATH = Path(__file__).parent / "report_config" / "market_place_config.yaml"
with open(MARKET_PLACE_CONFIG_FILE_PATH, "r") as file:
    market_place_config = yaml.safe_load(file)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/119.0.0.0 Safari/537.36"
)
STORAGE_STATE_PATH = Path(__file__).parent / "auth_state.json"
SCREENSHOT_DIR = Path(__file__).parent / "screenshots"

# Timing constants
WAIT_TIME = {
    "min": 500,
    "max": 2000,
    "verification": 5000,
    "page_load": 30000,
}

csrf_token = None
headers = {}


class AmazonAuthError(Exception):
    """Custom exception for Amazon authentication errors"""

    pass


def take_screenshot(page: Page, prefix: str) -> Path:
    """Take a screenshot of the current page state"""
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    screenshot_path = SCREENSHOT_DIR / f"screenshot_{prefix}_{datetime.now().strftime(r'%Y%m%d_%H%M%S')}.png"
    page.screenshot(path=str(screenshot_path), full_page=True)
    logger.info(f"Screenshot saved to: {screenshot_path}")
    return screenshot_path


def log_retry_attempt(retry_state: RetryCallState) -> None:
    """Log retry attempt number"""
    if retry_state.attempt_number > 0:
        logger.info(
            f"Retry attempt {retry_state.attempt_number} of {retry_state.retry_object.stop.max_attempt_number}"
        )


def handle_2FA(page: Page, otp_secret: str) -> None:
    """Handle the 2FA"""

    # Handle 2FA
    logger.info("Handling 2FA...")
    totp = pyotp.TOTP(otp_secret)
    otp = totp.now()
    print(f"OTP: {otp}")
    page.get_by_label("Enter OTP:").fill(otp)
    page.get_by_label("Sign in").click()


def handle_request(request):
    global csrf_token
    global headers
    if csrf_token is None and "anti-csrftoken-a2z" in request.headers:
        csrf_token = request.headers["anti-csrftoken-a2z"]
        logger.info(f"Found CSRF token: {csrf_token}")
        headers["anti-csrftoken-a2z"] = csrf_token


def setup_browser(playwright: Playwright, headless: bool):
    """Setup and return browser and context"""
    browser = playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-web-security",
            "--disable-features=IsolateOrigins,site-per-process",
        ],
    )

    context = browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        storage_state=STORAGE_STATE_PATH if STORAGE_STATE_PATH.exists() else None,
    )

    return browser, context


@retry(
    retry=retry_if_exception_type(AmazonAuthError),
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    before_sleep=log_retry_attempt,
)
def login_and_get_cookie(
    market_place: str,
    username: str,
    password: str,
    otp_secret: str,
    account: str,
    headless: bool = True,
    amazon_ads: bool = False,
    amazon_fulfillment: bool = False,
    context: Page = None,
) -> str:
    """Login to Amazon and get session cookie"""
    logger.info("Logging in to Amazon...")

    with sync_playwright() as p:
        browser, context = setup_browser(p, headless)
        context = browser.new_context(locale="en-US")
        page = context.new_page()
        global csrf_token
        global headers
        try:
            marketplace_config = market_place_config.get("marketplace_config", {}).get(market_place)

            # navigate to amazon seller central

            page.goto("https://sellercentral.amazon.com/")
            page.wait_for_load_state("networkidle")

            if page.get_by_role("link", name="Log in", exact=True).is_visible():
                page.get_by_role("link", name="Log in", exact=True).click()
                page.get_by_label("Email or mobile phone number").click(modifiers=["ControlOrMeta"])
                page.get_by_label("Email or mobile phone number").fill(username)
                page.get_by_label("Continue").click()
                page.get_by_label("Password").click()
                page.get_by_label("Password").fill(password)
                page.get_by_label("Sign in").click()

                handle_2FA(page=page, otp_secret=otp_secret)

                page.wait_for_timeout(5000)
                if not page.get_by_role("button", name=market_place, exact=True).is_visible():
                    page.get_by_role("button", name=account).click()
                page.get_by_role("button", name=market_place, exact=True).click()
                page.get_by_role("button", name="Select account").click()
                page.wait_for_timeout(8000)

                if page.get_by_role("heading", name="Sign in", exact=True).count() > 0:
                    page.get_by_label("Email or mobile phone number").click(modifiers=["ControlOrMeta"])
                    page.get_by_label("Email or mobile phone number").fill(username)
                    page.get_by_label("Continue").click()
                    page.get_by_label("Password").click()
                    page.get_by_label("Password").fill(password)
                    page.get_by_label("Sign in").click()
                    page.wait_for_timeout(30000)
                    handle_2FA(page=page, otp_secret=otp_secret)

                # Change the lanuguage
                try:
                    if market_place != "United States":
                        page.get_by_label("Language").locator("div").filter(has_text="EN").locator("div").click()
                        page.get_by_role(
                            "link",
                            name=f"English {marketplace_config['english_country']} ({marketplace_config['locale']})",
                        ).click()
                        logger.info(
                            f"Language changed to English {marketplace_config['english_country']} ({marketplace_config['locale']})"
                        )
                except:
                    logger.info(
                        f"Language Already Selected, English {marketplace_config['english_country']} ({marketplace_config['locale']})"
                    )

            else:
                logger.info("Already logged in, skipping login.")
                if page.get_by_role("button", name="Select account", exact=True).is_visible():
                    page.get_by_role("button", name=market_place).click()
                    page.get_by_role("button", name="Select account").click()

            if amazon_ads:
                page.get_by_label("Navigation menu").click()
                page.wait_for_timeout(20000)
                # Listen to all requests
                page.on("requestfinished", handle_request)

                page.locator("#sc-navbar-container").get_by_text("Reports", exact=True).click()
                page.get_by_role("link", name="Advertising Reports External").click()
                page.get_by_label("Sponsored ads reports", exact=True).click()

            if amazon_fulfillment:
                page.get_by_label("Navigation menu").click()
                page.wait_for_timeout(10000)

                # Listen to all requests
                page.on("requestfinished", handle_request)

                page.locator("#sc-navbar-container").get_by_text("Reports", exact=True).click()
                try:
                    page.get_by_role("link", name="Fulfillment Remove page from").click()
                except:
                    logger.info("Fulfillment Remove page from not found, trying another method")
                try:
                    page.get_by_role("link", name="Fulfilment by Amazon Remove").click()
                except:
                    logger.info("Fulfilment by Amazon Remove, trying another method")
                try:
                    page.get_by_role("link", name="Fulfillment Add page to").click()
                except:
                    logger.info("Fulfillment Add page to, trying another method")

                try:
                    page.get_by_text("Show more...").nth(1).click()
                except:
                    pass
                page.locator("#report-central-nav").get_by_role("link", name="All Orders").click()

            # Collect session cookies
            logger.info("Collecting session cookies...")
            page.wait_for_timeout(10000)

            # Get all cookies
            all_cookies = context.cookies()
            cookie = {cookie["name"]: cookie["value"] for cookie in all_cookies}

            # Cleanup
            if SCREENSHOT_DIR.exists():
                shutil.rmtree(SCREENSHOT_DIR)

            # Save browser state
            context.storage_state(path=STORAGE_STATE_PATH)

            return cookie, headers

        except Exception as e:
            logger.error(f"\n=== ERROR ===\nURL: {page.url}\nError: {str(e)}")
            take_screenshot(page, "error")
            raise AmazonAuthError(f"Login failed: {str(e)}")

        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    pass
