import argparse
import os
import random
import sys
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from urllib.parse import quote_plus

import pandas as pd
import undetected_chromedriver as uc
from loguru import logger
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


@dataclass
class ScraperConfig:
    email: str
    password: str
    titles: List[str]
    locations: List[str]
    max_pages_per_query: int
    min_delay: float
    max_delay: float
    rate_limit_pause_seconds: int
    output_file: str


def load_dotenv(dotenv_path: Optional[Path] = None) -> None:
    """Load key=value pairs from a local .env file into process env if missing."""
    path = dotenv_path or (Path(__file__).resolve().parent / ".env")
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def random_delay(min_seconds: float, max_seconds: float) -> None:
    sleep_for = random.uniform(min_seconds, max_seconds)
    logger.debug(f"Sleeping for {sleep_for:.2f}s to simulate human behavior.")
    time.sleep(sleep_for)


def human_type(element, text: str, min_key_delay: float = 0.05, max_key_delay: float = 0.2) -> None:
    for ch in text:
        element.send_keys(ch)
        time.sleep(random.uniform(min_key_delay, max_key_delay))


def human_scroll(driver, min_steps: int = 4, max_steps: int = 10) -> None:
    steps = random.randint(min_steps, max_steps)
    logger.debug(f"Human-like scrolling with {steps} step(s).")

    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(steps):
        scroll_by = random.randint(250, 900)
        driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_by)
        time.sleep(random.uniform(0.5, 1.6))

        current_height = driver.execute_script("return document.body.scrollHeight")
        if current_height > last_height:
            last_height = current_height

    # Slight upward movement to mimic natural reading behavior.
    driver.execute_script("window.scrollBy(0, -arguments[0]);", random.randint(50, 220))
    time.sleep(random.uniform(0.4, 1.2))


def is_rate_limited(driver) -> bool:
    indicators = [
        "too many requests",
        "unusual activity",
        "security verification",
        "verify your identity",
        "checkpoint",
        "captcha",
        "temporarily restricted",
    ]
    page_text = driver.page_source.lower()
    url = driver.current_url.lower()
    url_indicators = ("checkpoint", "challenge", "captcha", "login-submit")

    return any(x in page_text for x in indicators) or any(x in url for x in url_indicators)


def handle_rate_limit(driver, pause_seconds: int) -> None:
    if not is_rate_limited(driver):
        return

    backoff = int(pause_seconds * random.uniform(1.0, 1.6))
    logger.warning(
        f"Potential LinkedIn rate limit/challenge detected. Pausing scraper for {backoff} seconds."
    )
    time.sleep(backoff)

    # Try to continue once more after cooldown.
    if is_rate_limited(driver):
        logger.warning(
            "Rate-limit indicators still present. Waiting for manual verification "
            "(you can solve challenge in the opened browser)."
        )
        time.sleep(max(backoff, 120))


def create_driver(headless: bool = False):
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--lang=en-US")
    if headless:
        options.add_argument("--headless=new")

    driver = uc.Chrome(options=options, use_subprocess=True)
    driver.set_page_load_timeout(60)
    return driver


def secure_login(driver, config: ScraperConfig) -> None:
    logger.info("Opening LinkedIn login page.")
    driver.get("https://www.linkedin.com/login")
    wait = WebDriverWait(driver, 30)

    try:
        username_input = wait.until(EC.presence_of_element_located((By.ID, "username")))
        password_input = wait.until(EC.presence_of_element_located((By.ID, "password")))
    except TimeoutException as exc:
        raise RuntimeError("Could not locate LinkedIn login form.") from exc

    username_input.clear()
    human_type(username_input, config.email)
    random_delay(config.min_delay, config.max_delay)

    password_input.clear()
    human_type(password_input, config.password)
    random_delay(config.min_delay, config.max_delay)
    password_input.send_keys(Keys.ENTER)

    random_delay(config.min_delay + 1, config.max_delay + 2)
    handle_rate_limit(driver, config.rate_limit_pause_seconds)

    if "feed" in driver.current_url.lower() or "linkedin.com/" in driver.current_url.lower():
        logger.info("Login submitted. Session appears active.")
    else:
        logger.warning("Unexpected URL after login. Please verify login status manually.")


def extract_text_safe(element, selectors: List[str]) -> str:
    for selector in selectors:
        try:
            text = element.find_element(By.CSS_SELECTOR, selector).text.strip()
            if text:
                return text
        except NoSuchElementException:
            continue
    return ""


def extract_leads_from_page(driver) -> List[Dict[str, str]]:
    leads: List[Dict[str, str]] = []
    cards = driver.find_elements(By.CSS_SELECTOR, "li.reusable-search__result-container")

    if not cards:
        cards = driver.find_elements(By.CSS_SELECTOR, "div.entity-result")

    for card in cards:
        try:
            profile_element = card.find_element(By.CSS_SELECTOR, "a.app-aware-link")
            profile_url = profile_element.get_attribute("href") or ""
            if "/in/" not in profile_url:
                continue
        except NoSuchElementException:
            continue

        name = extract_text_safe(
            card,
            [
                "span.entity-result__title-text a span[aria-hidden='true']",
                ".entity-result__title-text a span",
                ".t-16.t-black.t-bold",
            ],
        )
        job_title = extract_text_safe(
            card,
            [
                ".entity-result__primary-subtitle",
                ".t-14.t-black.t-normal",
            ],
        )
        company = extract_text_safe(
            card,
            [
                ".entity-result__secondary-subtitle",
                ".t-14.t-normal",
            ],
        )

        leads.append(
            {
                "Name": name or "N/A",
                "Job Title": job_title or "N/A",
                "Company": company or "N/A",
                "Profile URL": profile_url.split("?")[0],
            }
        )
    return leads


def open_search(driver, title: str, location: str) -> None:
    query = quote_plus(f"{title} {location}".strip())
    url = f"https://www.linkedin.com/search/results/people/?keywords={query}&origin=GLOBAL_SEARCH_HEADER"
    logger.info(f"Searching leads for title='{title}' and location='{location}'.")
    driver.get(url)


def go_to_next_page(driver) -> bool:
    try:
        next_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "button[aria-label='Next'], button.artdeco-pagination__button--next")
            )
        )
        if not next_button.is_enabled():
            return False

        ActionChains(driver).move_to_element(next_button).pause(random.uniform(0.2, 0.8)).click().perform()
        return True
    except (TimeoutException, NoSuchElementException):
        return False


def scrape_leads(driver, config: ScraperConfig) -> List[Dict[str, str]]:
    all_leads: List[Dict[str, str]] = []
    seen_urls: Set[str] = set()
    wait = WebDriverWait(driver, 20)

    for title in config.titles:
        for location in config.locations:
            open_search(driver, title, location)
            random_delay(config.min_delay + 1, config.max_delay + 2)
            handle_rate_limit(driver, config.rate_limit_pause_seconds)

            for page in range(1, config.max_pages_per_query + 1):
                logger.info(f"Scraping page {page}/{config.max_pages_per_query} for '{title}' in '{location}'.")

                try:
                    wait.until(
                        EC.presence_of_element_located(
                            (
                                By.CSS_SELECTOR,
                                "li.reusable-search__result-container, div.entity-result, .search-results-container",
                            )
                        )
                    )
                except TimeoutException:
                    logger.warning("Search results did not load in time.")
                    handle_rate_limit(driver, config.rate_limit_pause_seconds)

                human_scroll(driver)
                random_delay(config.min_delay, config.max_delay)

                leads = extract_leads_from_page(driver)
                new_count = 0
                for lead in leads:
                    url = lead["Profile URL"]
                    if url not in seen_urls:
                        seen_urls.add(url)
                        all_leads.append(lead)
                        new_count += 1

                logger.info(f"Found {len(leads)} leads on page, {new_count} new, total={len(all_leads)}.")
                handle_rate_limit(driver, config.rate_limit_pause_seconds)

                if page == config.max_pages_per_query:
                    break
                if not go_to_next_page(driver):
                    logger.info("No next page button found or no more pages available.")
                    break
                random_delay(config.min_delay + 1, config.max_delay + 3)

    return all_leads


def save_results(leads: List[Dict[str, str]], output_file: str) -> None:
    if not leads:
        logger.warning("No leads collected; output file will still be created with headers.")

    df = pd.DataFrame(leads, columns=["Name", "Job Title", "Company", "Profile URL"])
    # Keep Excel as the primary output as requested, but give a clearer error if the engine is missing.
    try:
        df.to_excel(output_file, index=False)
    except ModuleNotFoundError as exc:
        logger.error(
            f"Failed to write Excel file '{output_file}'. Missing Excel engine dependency. "
            "Install with: pip install openpyxl"
        )
        raise exc

    csv_path = output_file.rsplit(".", 1)[0] + ".csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.success(f"Saved {len(df)} lead(s) to '{output_file}' and '{csv_path}'.")


def generate_demo_leads(titles: List[str], locations: List[str], count: int) -> List[Dict[str, str]]:
    first_names = ["Alex", "Sam", "Taylor", "Jordan", "Casey", "Riley", "Morgan", "Jamie", "Avery", "Cameron"]
    last_names = ["Johnson", "Smith", "Brown", "Williams", "Miller", "Davis", "Wilson", "Moore", "Taylor", "Anderson"]
    companies = ["Acme Inc", "Globex", "Initech", "Umbrella Corp", "Stark Industries", "Wayne Enterprises"]

    titles = titles or ["Sales Manager"]
    locations = locations or ["United States"]

    leads: List[Dict[str, str]] = []
    for i in range(1, max(1, count) + 1):
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        title = random.choice(titles)
        location = random.choice(locations)
        company = random.choice(companies)
        slug = f"demo-lead-{i:03d}".lower()
        leads.append(
            {
                "Name": name,
                "Job Title": f"{title} ({location})",
                "Company": company,
                "Profile URL": f"https://www.linkedin.com/in/{slug}/",
            }
        )
    return leads


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="LinkedIn B2B Lead Scraper (Selenium + undetected-chromedriver)."
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run without LinkedIn login: generates demo leads and writes output files (for screenshots/tests).",
    )
    parser.add_argument(
        "--demo-count",
        type=int,
        default=25,
        help="Number of demo leads to generate when --demo is set.",
    )
    parser.add_argument(
        "--titles",
        nargs="+",
        default=["Sales Manager"],
        help="Job titles to search for. Example: --titles 'Sales Manager' 'Business Development Manager'",
    )
    parser.add_argument(
        "--locations",
        nargs="+",
        default=["United States"],
        help="Locations to search in. Example: --locations 'New York' 'California'",
    )
    parser.add_argument("--max-pages", type=int, default=3, help="Max pages per title/location query.")
    parser.add_argument("--min-delay", type=float, default=1.3, help="Minimum random delay between actions.")
    parser.add_argument("--max-delay", type=float, default=3.8, help="Maximum random delay between actions.")
    parser.add_argument(
        "--rate-limit-pause",
        type=int,
        default=int(os.getenv("RATE_LIMIT_PAUSE_SECONDS", "240")),
        help="Pause duration (seconds) when rate-limit indicators are detected.",
    )
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode.")
    parser.add_argument("--output", default="linkedin_leads.xlsx", help="Output Excel file name.")
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> ScraperConfig:
    email = os.getenv("LINKEDIN_EMAIL", "").strip()
    password = os.getenv("LINKEDIN_PASSWORD", "").strip()
    if not email or not password:
        raise ValueError(
            "Missing credentials. Set LINKEDIN_EMAIL and LINKEDIN_PASSWORD in environment "
            "variables or in a local .env file."
        )

    if args.min_delay <= 0 or args.max_delay <= 0 or args.min_delay >= args.max_delay:
        raise ValueError("Invalid delay settings. Ensure 0 < min-delay < max-delay.")

    return ScraperConfig(
        email=email,
        password=password,
        titles=args.titles,
        locations=args.locations,
        max_pages_per_query=max(1, args.max_pages),
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        rate_limit_pause_seconds=max(60, args.rate_limit_pause),
        output_file=args.output,
    )


def setup_logger() -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("linkedin_scraper.log", rotation="5 MB", retention=3, level="DEBUG")


def main() -> None:
    setup_logger()
    load_dotenv()
    args = parse_args()

    if args.demo:
        logger.info("Running in DEMO mode (no LinkedIn login, no browser automation).")
        random_delay(0.4, 1.0)
        leads = generate_demo_leads(args.titles, args.locations, args.demo_count)
        save_results(leads, args.output)
        logger.success("Demo run completed successfully.")
        return

    try:
        config = build_config(args)
    except ValueError as exc:
        msg = str(exc)
        if "Missing credentials" in msg:
            # Friendly behavior: if user didn't pass --demo but also didn't provide credentials,
            # automatically run demo mode so they can still validate the script end-to-end.
            logger.warning(msg)
            logger.info(
                "No credentials detected; automatically switching to DEMO mode. "
                "To run the real scraper, set LINKEDIN_EMAIL/LINKEDIN_PASSWORD (or .env)."
            )
            leads = generate_demo_leads(args.titles, args.locations, args.demo_count)
            save_results(leads, args.output)
            logger.success("Demo run completed successfully.")
            return

        logger.error(msg)
        sys.exit(1)

    logger.info("Starting LinkedIn B2B lead scraper.")
    driver = create_driver(headless=args.headless)

    try:
        secure_login(driver, config)
        leads = scrape_leads(driver, config)
        save_results(leads, config.output_file)
        logger.success("Scraping process completed successfully.")
    except Exception as exc:
        logger.exception(f"Scraper failed: {exc}")
        raise
    finally:
        random_delay(1.0, 2.0)
        driver.quit()


if __name__ == "__main__":
    main()
