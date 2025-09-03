import requests
import os

# from pathlib import Path


def get_id_from_api(url):
    """Get community ID from API URL with error handling"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json().get("id")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching community ID from {url}: {e}")
        return None


def setup_for_rdm_api_access(TESTING_MODE=True, GA_MODE=False):

    # TODO: remove need for mapping file and url list
    # fetch that info from RDM

    TESTING_MODE = TESTING_MODE  # Set to False for production
    GA_MODE = GA_MODE  # Set to True for GitHub Actions mode

    # see Stackoverflow: https://stackoverflow.com/a/66593457 about use in GitHub Actions
    # variable/secret needs to be passed in the GitHub Action
    # - name: Test env vars for python
    #     run: TEST_SECRET=${{ secrets.MY_TOKEN }} python -c 'import os;print(os.environ['TEST_SECRET'])

    if TESTING_MODE:
        RDM_API_URL = "https://test.researchdata.tuwien.ac.at/api"
        MAPPING_FILE = "work_id_record_id_mapping_TESTING.csv"
        URL_LIST_FILE = "url_list_TESTING.csv"

        ELAUTE_COMMUNITY_ID = get_id_from_api(
            f"{RDM_API_URL}/communities/e-laute-test"
        )
        if GA_MODE:
            print("🧪 Running in GitHubActions TESTING mode")
            RDM_API_TOKEN = os.environ["RDM_API_TEST_TOKEN_JJ"]
        else:
            from dotenv import load_dotenv

            load_dotenv()
            print("🧪 Running in local TESTING mode")
            RDM_API_TOKEN = os.getenv("RDM_TEST_API_TOKEN")

    else:
        RDM_API_URL = "https://researchdata.tuwien.ac.at/api"
        MAPPING_FILE = "work_id_record_id_mapping.csv"
        URL_LIST_FILE = "url_list.csv"
        ELAUTE_COMMUNITY_ID = get_id_from_api(
            f"{RDM_API_URL}/communities/e-laute"
        )

        if GA_MODE:
            print(" 🚀 Running in GitHubActions PRODUCTION mode")
            RDM_API_TOKEN = os.environ["RDM_API_TOKEN_JJ"]

        else:
            from dotenv import load_dotenv

            load_dotenv()
            print("🚀 Running in local PRODUCTION mode")
            RDM_API_TOKEN = os.getenv("RDM_API_TOKEN")

    print(f"Using mapping file: {MAPPING_FILE}")
    print(f"Using URL list file: {URL_LIST_FILE}")
    print(f"Using API URL: {RDM_API_URL}")

    if ELAUTE_COMMUNITY_ID:
        print(f"Community ID: {ELAUTE_COMMUNITY_ID}")
    else:
        print("Warning: Could not fetch community ID")

    if GA_MODE:
        # this is equal to the home dir in the sources repository (so where the files that should be uploaded are located)
        FILES_PATH = "./caller-repo/"  # TODO: with or without Path??
        # FILES_PATH = Path("./caller-repo/")
    else:
        FILES_PATH = "files/"

    return (
        RDM_API_URL,
        RDM_API_TOKEN,
        FILES_PATH,
        ELAUTE_COMMUNITY_ID,
        MAPPING_FILE,
        URL_LIST_FILE,
    )


# Utility: make HTML link
def make_html_link(url):
    return f'<a href="{url}" target="_blank">{url}</a>'


# Utility: look up source title (stub, replace with actual lookup if needed)
def look_up_source_title(sources_table, source_id):
    # This should look up the title from a table or database; placeholder:
    title_series = sources_table.loc[
        sources_table["source_id"] == source_id, "Title"
    ]
    if not title_series.empty:
        return title_series.values[0]
    return None


# Utility: look up source links (stub, replace with actual lookup if needed)
def look_up_source_links(sources_table, source_id):
    source_link = sources_table.loc[
        sources_table["source_id"] == source_id,
        "Source_link",
    ].values[0]
    rism = sources_table.loc[
        sources_table["source_id"] == source_id,
        "RISM_link",
    ].values[0]
    vd16 = sources_table.loc[
        sources_table["source_id"] == source_id,
        "VD_16",
    ].values[0]

    links = []
    if source_link:
        links.append(source_link)
    if rism:
        links.append(rism)
    if vd16:
        links.append(vd16)

    return links


def create_related_identifiers(links):
    related_identifiers = []
    for link in links:
        related_identifiers.append(
            {
                "identifier": link,
                "relation_type": {
                    "id": "ispartof",
                    "title": {"en": "Is part of"},
                },
                "resource_type": {
                    "id": "other",
                    "title": {"de": "Anderes", "en": "Other"},
                },
                "scheme": "url",
            },
        )
    return related_identifiers
