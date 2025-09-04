import requests
import os
import json

import pandas as pd

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

    if GA_MODE:
        # this is equal to the home dir in the sources repository (so where the files that should be uploaded are located)
        FILES_PATH = "./caller-repo/"  # TODO: with or without Path??
        # FILES_PATH = Path("./caller-repo/")
    else:
        FILES_PATH = "scripts/upload_to_RDM/files/"

    return (
        RDM_API_URL,
        RDM_API_TOKEN,
        FILES_PATH,
        ELAUTE_COMMUNITY_ID,
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


def set_headers(RDM_API_TOKEN):

    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RDM_API_TOKEN}",
    }
    fh = {
        "Accept": "application/json",
        "Content-Type": "application/octet-stream",
        "Authorization": f"Bearer {RDM_API_TOKEN}",
    }
    return h, fh


def get_records_from_RDM(RDM_API_TOKEN, RDM_API_URL, ELAUTE_COMMUNITY_ID):
    """
    Fetch records from the RDM API.
    """
    h, fh = set_headers(RDM_API_TOKEN)
    response = requests.get(
        f"{RDM_API_URL}/communities/{ELAUTE_COMMUNITY_ID}/records",
        headers=h,
    )

    if not response.status_code == 200:
        print(f"Error fetching records from RDM: {response.status_code}")
        return None

    records = []
    hits = response.json().get("hits", {}).get("hits", [])
    for hit in hits:
        record_id = hit.get("id")
        parent_id = hit.get("parent", {}).get("id")
        file_count = hit.get("files", {}).get("count")
        created = hit.get("created")
        updated = hit.get("updated")
        # Try to extract elaute_id from identifiers (other)
        elaute_id = None
        metadata = hit.get("metadata", {})
        identifiers = metadata.get("identifiers")
        for ident in identifiers or []:
            if ident.get("scheme") == "other":
                elaute_id = ident.get("identifier")
                break
        if not elaute_id:
            print(f"Unknown E-LAUTE ID for record {record_id}")
        records.append(
            {
                "elaute_id": elaute_id,
                "record_id": record_id,
                "parent_id": parent_id,
                "file_count": file_count,
                "created": created,
                "updated": updated,
            }
        )
    return pd.DataFrame(records)


def upload_to_rdm(
    metadata,
    elaute_id,
    file_paths,
    RDM_API_TOKEN,
    RDM_API_URL,
    ELAUTE_COMMUNITY_ID,
    record_id=None,
    draft_one=False,
):
    new_upload = record_id is None

    failed_uploads = []
    print(f"Processing {elaute_id}: {len(file_paths)} files")
    h, fh = set_headers(RDM_API_TOKEN)

    print("record_id:", record_id)

    if not new_upload:
        # Create a new version/draft for the record
        r = requests.post(
            f"{RDM_API_URL}/records/{record_id}/versions", headers=h
        )
        if r.status_code != 201:
            print(
                f"Failed to create new version for record {record_id} (code: {r.status_code})"
            )
            failed_uploads.append(elaute_id)
            return failed_uploads  # Stop further processing

        new_version_data = r.json()
        new_record_id = new_version_data["id"]
        print(f"Created new version {new_record_id} for elaute_id {elaute_id}")

        # Update the draft with new metadata
        r = requests.put(
            f"{RDM_API_URL}/records/{new_record_id}/draft",
            data=json.dumps(metadata),
            headers=h,
        )
        if r.status_code != 200:
            print(
                f"Failed to update draft {new_record_id} (code: {r.status_code})"
            )
            failed_uploads.append(elaute_id)
            return failed_uploads

        # Use new_record_id for subsequent steps
        record_id = new_record_id
        # Get links from the draft update response
        links = r.json()["links"]
        record_id = r.json()["id"]
        print(links)
    else:
        # Create new draft record
        r = requests.post(
            f"{RDM_API_URL}/records", data=json.dumps(metadata), headers=h
        )
        assert (
            r.status_code == 201
        ), f"Failed to create record (code: {r.status_code})"
        links = r.json()["links"]
        record_id = r.json()["id"]
        print(links)

    # Upload each file individually
    i = 0
    for file_path in file_paths:
        filename = os.path.basename(file_path)

        # Initiate the file
        data = json.dumps([{"key": filename}])
        r = requests.post(links["files"], data=data, headers=h)
        assert (
            r.status_code == 201
        ), f"Failed to create file {filename} (code: {r.status_code})"

        file_links = r.json()["entries"][i]["links"]
        i += 1

        # Upload file content by streaming the data
        with open(file_path, "rb") as fp:
            r = requests.put(file_links["content"], data=fp, headers=fh)
        assert (
            r.status_code == 200
        ), f"Failed to upload file content {filename} (code: {r.status_code})"

        # Commit the file
        r = requests.post(file_links["commit"], headers=h)
        assert (
            r.status_code == 200
        ), f"Failed to commit file {filename} (code: {r.status_code})"

    # Add to E-LAUTE community
    if new_upload:
        if ELAUTE_COMMUNITY_ID:
            r = requests.put(
                f"{RDM_API_URL}/records/{record_id}/draft/review",
                headers=h,
                data=json.dumps(
                    {
                        "receiver": {"community": ELAUTE_COMMUNITY_ID},
                        "type": "community-submission",
                    }
                ),
            )
            assert (
                r.status_code == 200
            ), f"Failed to set review for record {record_id} (code: {r.status_code})"
        else:
            print(
                "Warning: ELAUTE_COMMUNITY_ID not set, skipping community submission"
            )
    # else:
    #     # if a record has already been published, it can be published again after changes without needing a review
    #     r = requests.post(
    #         f"{RDM_API_URL}/records/{record_id}/draft/actions/publish",
    #         headers=h,
    #     )
    #     if r.status_code != 202:
    #         print(
    #             f"Failed to publish record {record_id} (code: {r.status_code})"
    #         )

    # For production: create curation request and publish
    # Only trigger curation and submit-review if not in --draft-one mode
    if not draft_one:
        if new_upload:
            r = requests.post(
                f"{RDM_API_URL}/curations",
                headers=h,
                data=json.dumps({"topic": {"record": record_id}}),
            )
            assert (
                r.status_code == 201
            ), f"Failed to create curation for record {record_id} (code: {r.status_code})"

        # Submit the review for the record draft
        r = requests.post(
            f"{RDM_API_URL}/records/{record_id}/draft/actions/submit-review",
            headers=h,
        )
        if not r.status_code == 202:
            print(
                f"Failed to submit review for record {record_id} (code: {r.status_code})"
            )
            failed_uploads.append(elaute_id)

    return failed_uploads


def normalize_for_comparison(obj):
    """Normalize data structures for more reliable comparison"""
    if obj is None:
        return None
    elif isinstance(obj, str):
        normalized = obj.strip()
        return None if normalized == "" else normalized
    elif isinstance(obj, list):
        normalized_items = []
        for item in obj:
            if item is not None:
                normalized_item = normalize_for_comparison(item)
                if normalized_item is not None:
                    normalized_items.append(normalized_item)

        try:
            return sorted(
                normalized_items,
                key=lambda x: (
                    json.dumps(x, sort_keys=True)
                    if isinstance(x, dict)
                    else str(x)
                ),
            )
        except (TypeError, ValueError):
            return normalized_items
    elif isinstance(obj, dict):
        normalized_dict = {}
        for k, v in obj.items():
            normalized_value = normalize_for_comparison(v)
            if normalized_value is not None:
                normalized_dict[k] = normalized_value
        return normalized_dict if normalized_dict else None
    else:
        return obj


def deep_compare_metadata(current_value, new_value):
    """Compare two metadata values with normalization"""
    normalized_current = normalize_for_comparison(current_value)
    normalized_new = normalize_for_comparison(new_value)

    if normalized_current is None and normalized_new is None:
        return True
    if normalized_current is None or normalized_new is None:
        return False

    if isinstance(normalized_current, dict) and isinstance(
        normalized_new, dict
    ):
        try:
            current_json = json.dumps(
                normalized_current,
                sort_keys=True,
                separators=(",", ":"),
            )
            new_json = json.dumps(
                normalized_new,
                sort_keys=True,
                separators=(",", ":"),
            )
            return current_json == new_json
        except (TypeError, ValueError):
            if set(normalized_current.keys()) != set(normalized_new.keys()):
                return False
            for key in normalized_current.keys():
                if not deep_compare_metadata(
                    normalized_current[key], normalized_new[key]
                ):
                    return False
            return True

    if isinstance(normalized_current, list) and isinstance(
        normalized_new, list
    ):
        current_set = set()
        new_set = set()

        for item in normalized_current:
            try:
                item_str = (
                    json.dumps(item, sort_keys=True)
                    if isinstance(item, dict)
                    else str(item)
                )
                current_set.add(item_str)
            except (TypeError, ValueError):
                current_set.add(str(item))

        for item in normalized_new:
            try:
                item_str = (
                    json.dumps(item, sort_keys=True)
                    if isinstance(item, dict)
                    else str(item)
                )
                new_set.add(item_str)
            except (TypeError, ValueError):
                new_set.add(str(item))

        return current_set == new_set

    try:
        current_json = json.dumps(
            normalized_current,
            sort_keys=True,
            separators=(",", ":"),
        )
        new_json = json.dumps(
            normalized_new, sort_keys=True, separators=(",", ":")
        )
        return current_json == new_json
    except (TypeError, ValueError):
        return normalized_current == normalized_new


# if __name__ == "__main__":
#     pass
