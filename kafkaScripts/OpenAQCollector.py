import asyncio
import aiohttp
from typing import List, Dict, Optional, Generator
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("spark_kafka.log"),
        logging.StreamHandler()
    ]
)

class OpenAQDataCollector:
    """
    A robust OpenAQ Data Collector for fetching data with rate limit management.
    """
    def __init__(self, api_key: str, api_base_url: str = 'https://api.openaq.org/v3'):
        """
        Initializes the OpenAQDataCollector with API key and base URL.
        
        :param api_key: API key for authentication.
        :param api_base_url: Base URL for OpenAQ API.
        """
        self.base_url = api_base_url
        self.headers = {
            "X-API-Key": api_key,
            "Accept": "application/json"
        }

        # Rate limit settings
        self.max_concurrent_requests = 10
        self.lock = asyncio.Lock()
        self.next_reset = time.time()
        self.remaining_requests = 60
        self.batch_delay = 1.0
        self.locations = None

    async def _rate_limiter(self):
        """
        Ensures compliance with API rate limits using dynamic wait times.
        """
        async with self.lock:
            now = time.time()
            if self.remaining_requests <= 0:
                wait_time = max(self.next_reset-now, 0)
                logging.warning(f"Rate limit reached. Waiting {wait_time:.2f} seconds.")
                await asyncio.sleep(wait_time + 0.1)  # Buffer to avoid early requests
                self.remaining_requests = 60

            self.remaining_requests -= 1
            logging.info(f"Requests remaining: {self.remaining_requests}")

    async def _make_request(self, session: aiohttp.ClientSession, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Sends a GET request to the API while respecting rate limits.
        
        :param session: AIOHTTP session for making requests.
        :param url: API endpoint URL.
        :param params: Query parameters for the request.
        :return: Parsed JSON response or None on failure.
        """
        await self._rate_limiter()
        try:
            async with session.get(url, headers=self.headers, params=params) as response:
                response.raise_for_status()

                async with self.lock:
                    self.remaining_requests = int(response.headers.get('x-ratelimit-remaining', self.remaining_requests))
                    self.next_reset = int(response.headers.get('x-ratelimit-reset', time.time() + 60))
                    logging.info(f"Rate limit reset at: {self.next_reset}")

                return await response.json()

        except aiohttp.ClientError as e:
            logging.error(f"Error during request to {url}: {e}")
            return None

    async def get_country_data(self, country_id: int) -> Optional[Dict]:
        """
        Fetches raw country data from the API.

        :param country_id: ID of the country to fetch data for.
        :return: Country data as a dictionary or None if not found.
        """
        url = f"{self.base_url}/countries/{country_id}"
        async with aiohttp.ClientSession() as session:
            response = await self._make_request(session, url)

        if not response or 'results' not in response:
            logging.warning(f"No data found for country ID {country_id}.")
            return None

        return response.get('results', [None])[0]

    async def get_parameter_data(self, parameter_ids: List[int]) -> List[Dict]:
        """
        Fetches parameter details from the API for the given parameter IDs.

        :param parameter_ids: List of parameter IDs to fetch.
        :return: List of parameter details.
        """
        tasks = []
        async with aiohttp.ClientSession() as session:
            for param_id in parameter_ids:
                url = f"{self.base_url}/parameters/{param_id}"
                tasks.append(self._make_request(session, url))

            responses = await asyncio.gather(*tasks, return_exceptions=True)

        return [response['results'][0] for response in responses if isinstance(response, dict) and 'results' in response]

    async def get_locations_data(self, country_id: int) -> List[Dict]:
        """
        Fetches location data for a given country ID.

        :param country_id: ID of the country to fetch locations for.
        :return: List of location data.
        """
        endpoint = f"{self.base_url}/locations"
        locations = []
        page = 1

        async with aiohttp.ClientSession() as session:
            while True:
                params = {"countries_id": country_id, "limit": 1000, "page": page}
                response = await self._make_request(session, endpoint, params)

                if not response:
                    break

                locations.extend(response.get("results", []))

                meta = response.get("meta", {})
                if meta.get("found", 0) <= meta.get("limit", 1000) * page:
                    break
                page += 1

        self.locations = locations
        return locations

    async def get_latest_measurements(self, locations: List[Dict]):
        """
        Fetches the latest measurements incrementally for the provided locations with pagination.

        :param locations: List of locations to fetch measurements for.
        :yield: Incremental batches of latest measurements.
        """
        if not locations:
            logging.warning("No locations provided for fetching measurements.")
            return

        async with aiohttp.ClientSession() as session:
            for i, location in enumerate(locations):
                location_id = location.get("id")
                if not location_id:
                    logging.warning(f"Missing id for location: {location}")
                    continue

                # Initialize pagination for this location
                page = 1
                while True:
                    url = f"{self.base_url}/locations/{location_id}/latest"
                    params = {"limit": 1000, "page": page}

                    # Rate limit check before making the request
                    async with self.lock:
                        if self.remaining_requests <= 0:
                            wait_time = self.next_reset
                            logging.warning(f"Rate limit reached. Waiting {wait_time:.2f} seconds.")
                            await asyncio.sleep(wait_time + 0.1)
                            self.remaining_requests = 60  # Reset after wait
                        self.remaining_requests -= 1

                    # Fetch the data for the current page
                    response = await self._make_request(session, url, params)

                    if not response or "results" not in response:
                        logging.info(f"No more data for location {location_id} on page {page}.")
                        break  # Stop if no data is returned

                    # Process and yield the results
                    measurements = response.get("results", [])
                    if measurements:
                        yield measurements

                    # Pagination metadata to check if more pages are available
                    meta = response.get("meta", {})
                    if meta.get("found", 0) <= meta.get("limit", 1000) * page:
                        logging.info(f"All pages habe been processed. Finished at page : {page}")
                        break  # Exit if all pages have been processed

                    # Increment the page and continue
                    page += 1

                    # Add a small delay between requests to avoid overwhelming the API
                    await asyncio.sleep(self.batch_delay)