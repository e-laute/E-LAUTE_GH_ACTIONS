import pandas as pd
import os
import sys
from lxml import etree

from pathlib import Path

import requests
import json

import re

from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


# Configuration based on TESTING_MODE
TESTING_MODE = True  # Set to False for production
GA_MODE = False


def get_id_from_api(url):
    """Get community ID from API URL with error handling"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json().get("id")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching community ID from {url}: {e}")
        return None


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
        print("🧪 Running in local TESTING mode")
        RDM_API_TOKEN = os.getenv("RDM_TEST_API_TOKEN")

else:
    RDM_API_URL = "https://researchdata.tuwien.ac.at/api"
    MAPPING_FILE = "work_id_record_id_mapping.csv"
    URL_LIST_FILE = "url_list.csv"
    ELAUTE_COMMUNITY_ID = get_id_from_api(f"{RDM_API_URL}/communities/e-laute")

    if GA_MODE:
        print(" 🚀 Running in GitHubActions PRODUCTION mode")
        RDM_API_TOKEN = os.environ["RDM_API_TOKEN_JJ"]

    else:
        print("🚀 Running in local PRODUCTION mode")
        RDM_API_TOKEN = os.getenv("RDM_API_TOKEN")


# see Stackoverflow: https://stackoverflow.com/a/66593457
# needs to be passed in the GitHub Action
# - name: Test env vars for python
#     run: TEST_SECRET=${{ secrets.MY_TOKEN }} python -c 'import os;print(os.environ['TEST_SECRET'])

# RDM_API_URL = os.environ["RDM_API_URL"]
# RDM_API_TOKEN = os.environ["RDM_API_TOKEN"]

print(f"Using mapping file: {MAPPING_FILE}")
print(f"Using URL list file: {URL_LIST_FILE}")
print(f"Using API URL: {RDM_API_URL}")
if ELAUTE_COMMUNITY_ID:
    print(f"Community ID: {ELAUTE_COMMUNITY_ID}")
else:
    print("Warning: Could not fetch community ID")


if GA_MODE:
    # this is equal to the home dir in the sources repository (so where the files that should be uploaded are located)
    MEI_FILES_PATH = input_path = Path("./caller-repo/")
else:
    MEI_FILES_PATH = "files"
    # TODO: adapt to accomodate reality in git repos
    # get the name of the repo and its subfolders and go over each of them

errors = []
metadata_df = pd.DataFrame()

# TODO: implement extraction of info about sources from knowledge graph/dbrepo and not from exel-file
sources_excel_df = pd.read_excel("tables/sources_table.xlsx")
sources_table = pd.DataFrame()
sources_table["source_id"] = sources_excel_df["ID"].fillna(
    sources_excel_df["Shelfmark"]
)
sources_table["source_name"] = sources_excel_df["Title"]
sources_table["source_link"] = sources_excel_df["Source_link"].fillna("")
sources_table["RISM_link"] = sources_excel_df["RISM_link"].fillna("")
sources_table["VD_16"] = sources_excel_df["VD_16"].fillna("")


def look_up_source_title(source_id):
    return sources_table.loc[
        sources_table["source_id"] == source_id,
        "source_name",
    ].values[0]


def make_html_link(url):
    return f'<a href="{url}" target="_blank">{url}</a>'


def look_up_source_links(source_id):
    source_link = sources_table.loc[
        sources_table["source_id"] == source_id,
        "source_link",
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


def get_metadata_df_from_mei(mei_file_path):
    try:
        with open(mei_file_path, "rb") as f:
            content = f.read()

        doc = etree.fromstring(content)

        # Define namespace for MEI
        ns = {"mei": "http://www.music-encoding.org/ns/mei"}

        # Extract basic metadata
        metadata = {}

        # Extract work ID from identifier - try multiple locations
        identifier_elem = doc.find(".//mei:identifier", ns)
        if identifier_elem is None:
            # Try alternative locations
            identifier_elem = doc.find(".//mei:work/mei:identifier", ns)
        if identifier_elem is not None:
            metadata["work_id"] = identifier_elem.text.strip()
        else:
            # Fallback - extract from composer field if it contains folio info
            composer_elem = doc.find(".//mei:composer", ns)
            if composer_elem is not None and composer_elem.text:
                # Extract work ID from composer text like "Judenkünig 1523: fol. 28r-28v"
                composer_text = composer_elem.text.strip()
                if "fol." in composer_text:
                    # Create work ID from composer info
                    parts = composer_text.split(":")
                    if len(parts) >= 2:
                        folio_part = (
                            parts[1]
                            .strip()
                            .replace("fol. ", "")
                            .replace(" ", "")
                        )
                        metadata["work_id"] = f"Jud_1523-2_{folio_part}"
                        metadata["fol_or_p"] = (
                            parts[1].strip().replace("fol. ", "")
                        )

        # Extract titles - try multiple locations
        main_title = doc.find('.//mei:title[@type="main"]', ns)
        if main_title is None:
            # Try simple title element
            main_title = doc.find(".//mei:title", ns)
        if main_title is not None:
            metadata["title"] = main_title.text.strip()

        # Try work title if main title not found
        if "title" not in metadata:
            work_title = doc.find(".//mei:work/mei:title", ns)
            if work_title is not None:
                metadata["title"] = work_title.text.strip()

        original_title = doc.find('.//mei:title[@type="original"]', ns)
        if original_title is not None:
            metadata["original_title"] = original_title.text.strip()

        normalized_title = doc.find('.//mei:title[@type="normalized"]', ns)
        if normalized_title is not None:
            metadata["normalized_title"] = normalized_title.text.strip()

        # Extract publication date
        pub_date = doc.find(".//mei:pubStmt/mei:date[@isodate]", ns)
        if pub_date is not None:
            metadata["publication_date"] = pub_date.get("isodate")

        # Extract folio/page information if not already extracted
        if "fol_or_p" not in metadata:
            biblscope = doc.find(".//mei:biblScope", ns)
            if biblscope is not None:
                metadata["fol_or_p"] = biblscope.text.strip()

        # Extract source ID from composer field if available
        if "source_id" not in metadata:
            composer_elem = doc.find(".//mei:composer", ns)
            if composer_elem is not None and composer_elem.text:
                composer_text = composer_elem.text.strip()
                if "Judenkünig" in composer_text and "1523" in composer_text:
                    metadata["source_id"] = (
                        "A-Wn MS47356-8°"  # Default for Judenkünig 1523
                    )

        # Extract work ID from monograph identifier
        work_id = doc.find(".//mei:analytic/mei:identifier", ns)
        if work_id is not None:
            metadata["work_id"] = work_id.text.strip()
            # Extract source_id from work_id by removing everything after the last underscore
            work_id_text = work_id.text.strip()
            if "_" in work_id_text:
                metadata["source_id"] = work_id_text.rsplit("_", 1)[0]

        shelfmark = doc.find(".//mei:monogr/mei:identifier", ns)
        if shelfmark is not None:
            metadata["shelfmark"] = shelfmark.text.strip()

        book_title = doc.find(".//mei:monogr/mei:title", ns)
        if book_title is not None:
            metadata["book_title"] = book_title.text.strip()

        # Extract license information
        license_elem = doc.find(".//mei:useRestrict/mei:ref", ns)
        if license_elem is not None:
            metadata["license"] = license_elem.get("target")

        # Extract people and their roles
        people_data = []

        # Get all persName elements with roles
        person_elements = doc.findall(".//mei:persName[@role]", ns)

        for person in person_elements:
            auth_uri = person.get("auth.uri", "")
            # Extract ID from auth.uri (e.g., "https://e-laute.info/data/projectstaff/16" -> "projectstaff-16")
            pers_id = ""
            if auth_uri:
                uri_parts = auth_uri.split("/")
                if len(uri_parts) >= 2:
                    category = uri_parts[-2]  # e.g., "projectstaff"
                    number = uri_parts[-1]  # e.g., "16"
                    pers_id = f"{category}-{number}"
                else:
                    pers_id = auth_uri

            person_info = {
                "file_path": mei_file_path,
                "work_id": metadata.get("work_id", ""),
                "role": person.get("role", ""),
                "auth_uri": auth_uri,
                "pers_id": pers_id,
            }

            # Extract name parts
            forename = person.find("mei:foreName", ns)
            if forename is not None:
                person_info["first_name"] = forename.text.strip()

            famname = person.find("mei:famName", ns)
            if famname is not None:
                person_info["last_name"] = famname.text.strip()

            # Create full name
            full_name_parts = []
            if person_info.get("first_name"):
                full_name_parts.append(person_info["first_name"])
            if person_info.get("last_name"):
                full_name_parts.append(person_info["last_name"])
            person_info["full_name"] = " ".join(full_name_parts)

            people_data.append(person_info)

        # Extract corporate entities (funders, providers, etc.)
        corporate_data = []

        # Get all corpName elements with roles
        corp_elements = doc.findall(".//mei:corpName[@role]", ns)

        for corp in corp_elements:
            # Corporate entities use xml:id instead of auth.uri
            corp_id = corp.get("{http://www.w3.org/XML/1998/namespace}id", "")

            corp_info = {
                "file_path": mei_file_path,
                "work_id": metadata.get("work_id", ""),
                "role": corp.get("role", ""),
                "corp_id": corp_id,
            }

            # Extract organization name - could be in text or in ref/abbr/expan
            ref_elem = corp.find("mei:ref", ns)
            if ref_elem is not None:
                corp_info["url"] = ref_elem.get("target", "")

                # Check for abbreviation and expansion
                abbr_elem = ref_elem.find("mei:abbr", ns)
                if abbr_elem is not None:
                    corp_info["abbreviation"] = abbr_elem.text.strip()

                expan_elem = ref_elem.find("mei:expan", ns)
                if expan_elem is not None:
                    corp_info["full_name"] = expan_elem.text.strip()

                # If no abbr/expan, use the ref text
                if not corp_info.get("abbreviation") and not corp_info.get(
                    "full_name"
                ):
                    corp_info["name"] = (
                        ref_elem.text.strip() if ref_elem.text else ""
                    )
            else:
                # No ref element, use direct text content
                corp_info["name"] = corp.text.strip() if corp.text else ""

            # Use abbreviation as name if available, otherwise use full_name or name
            if not corp_info.get("name"):
                corp_info["name"] = (
                    corp_info.get("abbreviation")
                    or corp_info.get("full_name")
                    or ""
                )

            corporate_data.append(corp_info)

        # Create DataFrames
        people_df = pd.DataFrame(people_data)
        corporate_df = pd.DataFrame(corporate_data)

        # Create a single row metadata DataFrame
        metadata_row = pd.DataFrame([metadata])

        return metadata_row, people_df, corporate_df

    except etree.XMLSyntaxError as e:
        errors.append(f"Error parsing MEI file {mei_file_path}: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        errors.append(f"Error processing MEI file {mei_file_path}: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


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


def get_work_ids_from_files():
    """
    Scan all folders in MEI_FILES_PATH and extract unique work_ids.
    Extract everything up to and including the 'n' + number part.
    Example: Jud_1523-2_n10_18v_enc_dipl_GLT.mei -> Jud_1523-2_n10
    """
    work_ids = set()

    for root, dirs, files in os.walk(MEI_FILES_PATH):
        for file in files:
            if file.endswith(".mei"):
                # Remove .mei extension
                base_name = file.replace(".mei", "")

                # Use regex to find pattern: everything up to and including n + digits
                # Pattern matches: start of string, any characters, underscore, n, one or more digits
                match = re.match(r"^(.+_n\d+)", base_name)

                if match:
                    work_id = match.group(1)
                    work_ids.add(work_id)
                else:
                    # Fallback: if no 'n' pattern found, use the old method
                    if "_" in base_name:
                        work_id = base_name.rsplit("_", 1)[0]
                        work_ids.add(work_id)
                    else:
                        work_ids.add(base_name)

    return sorted(list(work_ids))


def get_files_for_work_id(work_id):
    """
    Get all MEI files that belong to a specific work_id.
    """
    import re

    matching_files = []

    for root, dirs, files in os.walk(MEI_FILES_PATH):
        for file in files:
            if file.endswith(".mei"):
                base_name = file.replace(".mei", "")

                # Use same regex pattern to extract work_id from filename
                match = re.match(r"^(.+_n\d+)", base_name)

                if match:
                    file_work_id = match.group(1)
                else:
                    # Fallback: use old method
                    if "_" in base_name:
                        file_work_id = base_name.rsplit("_", 1)[0]
                    else:
                        file_work_id = base_name

                if file_work_id == work_id:
                    matching_files.append(os.path.join(root, file))

    return matching_files


def combine_metadata_for_work_id(work_id, file_paths):
    """
    Extract and combine metadata from all files belonging to a work_id.
    Combines metadata without redundancies but preserves additional people/roles.
    """
    all_metadata = []
    all_people = pd.DataFrame()
    all_corporate = pd.DataFrame()

    for file_path in file_paths:
        metadata_df, people_df, corporate_df = get_metadata_df_from_mei(
            file_path
        )

        if not metadata_df.empty:
            all_metadata.append(metadata_df.iloc[0])

        if not people_df.empty:
            all_people = pd.concat([all_people, people_df], ignore_index=True)

        if not corporate_df.empty:
            all_corporate = pd.concat(
                [all_corporate, corporate_df], ignore_index=True
            )

    if not all_metadata:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Use the first file's metadata as the base, but merge key fields from other files
    combined_metadata = all_metadata[0].copy()

    # Merge additional metadata fields that might differ between files
    for metadata in all_metadata[1:]:
        # If base metadata is missing a field but another file has it, add it
        for key, value in metadata.items():
            if (
                pd.isna(combined_metadata.get(key))
                or combined_metadata.get(key) == ""
            ):
                if not pd.isna(value) and value != "":
                    combined_metadata[key] = value
                    print(f"    Merged field '{key}': {value}")

    # Special handling for publication_date: choose the latest/most recent date
    publication_dates = []
    for metadata in all_metadata:
        pub_date = metadata.get("publication_date")
        if pub_date and not pd.isna(pub_date) and pub_date != "":
            try:
                # Try to parse the date to ensure it's valid and for comparison
                parsed_date = datetime.strptime(pub_date, "%Y-%m-%d")
                publication_dates.append((pub_date, parsed_date))
                print(f"    Found publication date: {pub_date}")
            except ValueError:
                # If date parsing fails, skip this date
                print(f"    Skipping invalid date format: {pub_date}")
                continue

    if publication_dates:
        # Sort by parsed date and take the latest one
        latest_date = max(publication_dates, key=lambda x: x[1])
        combined_metadata["publication_date"] = latest_date[0]

    # Remove duplicates from people data - keep unique combinations of person + role
    if not all_people.empty:
        # Normalize names and roles for deduplication
        def normalize_person(row):
            # Lowercase and strip names and roles for robust deduplication
            full_name = str(row.get("full_name", "")).strip().lower()
            role = str(row.get("role", "")).strip().lower()
            return f"{full_name}-{role}"

        all_people["dedup_key"] = all_people.apply(normalize_person, axis=1)
        # Drop duplicates based on normalized key
        all_people = all_people.drop_duplicates(
            subset=["dedup_key"], keep="first"
        )
        all_people = all_people.drop("dedup_key", axis=1)

    # Remove duplicates from corporate data - keep unique combinations of organization + role
    if not all_corporate.empty:

        # Create a composite key for deduplication
        all_corporate["dedup_key"] = all_corporate.apply(
            lambda row: f"{row.get('name', '')}-{row.get('role', '')}", axis=1
        )

        # Keep first occurrence of each unique organization-role combination
        all_corporate = all_corporate.drop_duplicates(
            subset=["dedup_key"], keep="first"
        )
        all_corporate = all_corporate.drop("dedup_key", axis=1)

    return pd.DataFrame([combined_metadata]), all_people, all_corporate


def create_description_for_work(row, file_count):
    """
    Create description for a work with multiple files.
    """
    links_stringified = ""
    links = look_up_source_links(row["source_id"])
    for link in links if links else []:
        links_stringified += make_html_link(link) + ", "

    source_id = row["source_id"]
    work_number = row["work_id"].split("_")[-1]
    platform_link = make_html_link(
        f"https://edition.onb.ac.at/fedora/objects/o:lau.{source_id}/methods/sdef:TEI/get?mode={work_number}"
    )

    part1 = f"<h1>Transcriptions in MEI of a lute piece from the E-LAUTE project</h1><h2>Overview</h2><p>This dataset contains transcription files of the piece \"{row['title']}\", a 16th century lute music piece originally notated in lute tablature, created as part of the E-LAUTE project (<a href=\"https://e-laute.info/\">https://e-laute.info/</a>). The transcriptions preserve and make historical lute music from the German-speaking regions during 1450-1550 accessible.</p><p>They are based on the work with the title \"{row['title']}\" and the id \"{row['work_id']}\" in the e-lautedb. It is found on the page(s) or folio(s) {row['fol_or_p']} in the source \"{look_up_source_title(row['source_id'])}\" with the E-LAUTE source-id \"{row['source_id']}\" and the shelfmark {row['shelfmark']}.</p>"

    part4 = f"<p>Images of the original source and renderings of the transcriptions can be found on the E-LAUTE platform: {platform_link}.</p>"

    if links_stringified not in [None, ""]:
        part2 = f"<p>Links to the source: {links_stringified}.</p>"
    else:
        part2 = ""

    part3 = f'<h2>Dataset Contents</h2><p>This dataset includes {file_count} MEI files with different transcription variants (diplomatic/editorial versions in various tablature notations and common music notation).</p><h2>About the E-LAUTE Project</h2><p><strong>E-LAUTE: Electronic Linked Annotated Unified Tablature Edition - The Lute in the German-Speaking Area 1450-1550</strong></p><p>The E-LAUTE project creates innovative digital editions of lute tablatures from the German-speaking area between 1450 and 1550. This interdisciplinary "open knowledge platform" combines musicology, music practice, music informatics, and literary studies to transform traditional editions into collaborative research spaces.</p><p>For more information, visit the project website: <a href="https://e-laute.info/">https://e-laute.info/</a></p>'

    return part1 + part4 + part2 + part3


def fill_out_basic_metadata_for_work(
    metadata_row, people_df, corporate_df, file_count
):
    """
    Fill out metadata for RDM upload for a work with multiple files.
    """
    row = metadata_row.iloc[0]

    metadata = {
        "metadata": {
            "title": f'{row["title"]} ({row["work_id"]}) MEI Transcriptions',
            "creators": [],
            "contributors": [],
            "description": create_description_for_work(row, file_count),
            "publication_date": datetime.today().strftime("%Y-%m-%d"),
            "dates": [
                {
                    "date": row.get(
                        "publication_date",
                        datetime.today().strftime("%Y-%m-%d"),
                    ),
                    "description": "Creation date",
                    "type": {"id": "created", "title": {"en": "Created"}},
                }
            ],
            "publisher": "E-LAUTE",
            "references": [{"reference": "https://e-laute.info/"}],
            "related_identifiers": [],
            "resource_type": {
                "id": "dataset",
                "title": {"de": "Dataset", "en": "Dataset"},
            },
            "rights": [
                {
                    "description": {
                        "en": "Permits almost any use subject to providing credit and license notice. Frequently used for media assets and educational materials. The most common license for Open Access scientific publications. Not recommended for software."
                    },
                    "icon": "cc-by-sa-icon",
                    "id": "cc-by-sa-4.0",
                    "props": {
                        "scheme": "spdx",
                        "url": "https://creativecommons.org/licenses/by-sa/4.0/legalcode",
                    },
                    "title": {
                        "en": "Creative Commons Attribution Share Alike 4.0 International"
                    },
                }
            ],
        }
    }

    # Add people as creators and contributors (same logic as before)
    creator_names = set()
    contributor_names = set()

    # First pass: Add authors as creators
    for _, person in people_df.iterrows():
        if person.get("role") == "author":
            person_entry = {
                "person_or_org": {
                    "family_name": person.get("last_name", ""),
                    "given_name": person.get("first_name", ""),
                    "name": person.get("full_name", ""),
                    "type": "personal",
                }
            }
            person_entry["role"] = {"id": "other", "title": {"en": "Author"}}
            metadata["metadata"]["creators"].append(person_entry)
            creator_names.add(person.get("full_name", ""))

    # Second pass: Add intabulators as creators
    for _, person in people_df.iterrows():
        if (
            person.get("role") == "intabulator"
            and person.get("full_name", "") not in creator_names
        ):
            person_entry = {
                "person_or_org": {
                    "family_name": person.get("last_name", ""),
                    "given_name": person.get("first_name", ""),
                    "name": person.get("full_name", ""),
                    "type": "personal",
                }
            }
            person_entry["role"] = {
                "id": "other",
                "title": {"en": "Intabulator"},
            }
            metadata["metadata"]["creators"].append(person_entry)
            creator_names.add(person.get("full_name", ""))

    # Third pass: Add all other roles as contributors
    for _, person in people_df.iterrows():
        if person.get("role") not in ["author", "intabulator"]:
            # Create a unique key for this person-role combination
            person_role_key = (
                f"{person.get('full_name', '')}-{person.get('role', '')}"
            )

            if person_role_key not in contributor_names:
                person_entry = {
                    "person_or_org": {
                        "family_name": person.get("last_name", ""),
                        "given_name": person.get("first_name", ""),
                        "name": person.get("full_name", ""),
                        "type": "personal",
                    }
                }

                role_mapping = {
                    "meiEditor": {"id": "editor", "title": {"en": "Editor"}},
                    "fronimoEditor": {
                        "id": "editor",
                        "title": {"en": "Editor"},
                    },
                    "metadataContact": {
                        "id": "contactperson",
                        "title": {"en": "Contact person"},
                    },
                    "publisher": {"id": "other", "title": {"en": "Publisher"}},
                }

                person_entry["role"] = role_mapping.get(
                    person.get("role", ""),
                    {"id": "other", "title": {"en": "Other"}},
                )

                metadata["metadata"]["contributors"].append(person_entry)
                contributor_names.add(person_role_key)

    # Add source links as related identifiers
    links_to_source = look_up_source_links(row["source_id"])
    if links_to_source:
        metadata["metadata"]["related_identifiers"].extend(
            create_related_identifiers(links_to_source)
        )

    return metadata


def update_records_in_RDM(work_ids_to_update):
    """Update existing records in RDM if metadata has changed."""

    # HTTP Headers
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

    api_url = f"{RDM_API_URL}/records"

    # Load existing work_id to record_id mapping
    mapping_file = MAPPING_FILE
    if not os.path.exists(mapping_file):
        print(f"Mapping file {mapping_file} not found. No records to update.")
        return

    existing_mapping = pd.read_csv(mapping_file, sep=";")
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated_records = []
    failed_updates = []

    for work_id in work_ids_to_update:
        print(f"\n--- Checking for updates: {work_id} ---")

        # Check if work_id exists in mapping
        mapping_row = existing_mapping[existing_mapping["work_id"] == work_id]
        if mapping_row.empty:
            print(f"Work ID {work_id} not found in existing records. Skipping.")
            continue

        record_id = mapping_row.iloc[0]["record_id"]

        try:
            # Get files for this work_id and combine metadata
            file_paths = get_files_for_work_id(work_id)
            if not file_paths:
                print(f"No files found for work_id: {work_id}")
                continue

            metadata_df, people_df, corporate_df = combine_metadata_for_work_id(
                work_id, file_paths
            )

            if metadata_df.empty:
                print(f"Failed to extract metadata for work_id {work_id}")
                continue

            # Create new metadata structure
            new_metadata_structure = fill_out_basic_metadata_for_work(
                metadata_df, people_df, corporate_df, len(file_paths)
            )
            new_metadata = new_metadata_structure["metadata"]

            # Fetch current record metadata from RDM
            r = requests.get(f"{api_url}/{record_id}", headers=h)
            if r.status_code != 200:
                print(
                    f"Failed to fetch record {record_id} (code: {r.status_code})"
                )
                failed_updates.append(work_id)
                continue

            current_record = r.json()
            current_metadata = current_record.get("metadata", {})

            # Compare metadata (excluding auto-generated fields)
            fields_to_compare = [
                "title",
                "creators",
                "contributors",
                "description",
                "dates",
                "publisher",
                "references",
                "related_identifiers",
                "resource_type",
                "rights",
            ]

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
                        if set(normalized_current.keys()) != set(
                            normalized_new.keys()
                        ):
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

            # Check for metadata changes
            metadata_changed = False
            changes_detected = []

            for field in fields_to_compare:
                current_value = current_metadata.get(field)
                new_value = new_metadata.get(field)

                if not deep_compare_metadata(current_value, new_value):
                    metadata_changed = True
                    changes_detected.append(field)

            if not metadata_changed:
                continue

            print(
                f"Metadata changes detected for work_id {work_id} in fields: {', '.join(changes_detected)}"
            )

            # Create a new version/draft for the record
            r = requests.post(f"{api_url}/{record_id}/versions", headers=h)
            if r.status_code != 201:
                print(
                    f"Failed to create new version for record {record_id} (code: {r.status_code})"
                )
                failed_updates.append(work_id)
                continue

            new_version_data = r.json()
            new_record_id = new_version_data["id"]

            # Update the draft with new metadata
            r = requests.put(
                f"{api_url}/{new_record_id}/draft",
                data=json.dumps(new_metadata_structure),
                headers=h,
            )
            if r.status_code != 200:
                print(
                    f"Failed to update draft {new_record_id} (code: {r.status_code})"
                )
                failed_updates.append(work_id)
                continue

            # Update files - delete existing and upload new ones
            r = requests.delete(
                f"{api_url}/{new_record_id}/draft/files", headers=h
            )

            # Upload files
            file_entries = []
            for file_path in file_paths:
                file_entries.append({"key": os.path.basename(file_path)})

            # Initialize files
            data = json.dumps(file_entries)
            r = requests.post(
                f"{api_url}/{new_record_id}/draft/files",
                data=data,
                headers=h,
            )
            if r.status_code != 201:
                print(
                    f"Failed to initialize files for record {new_record_id} (code: {r.status_code})"
                )
                failed_updates.append(work_id)
                continue

            file_responses = r.json()["entries"]

            # Upload each file
            for i, file_path in enumerate(file_paths):
                file_links = file_responses[i]["links"]

                # Upload file content
                with open(file_path, "rb") as fp:
                    r = requests.put(file_links["content"], data=fp, headers=fh)
                if r.status_code != 200:
                    continue

                # Commit the file
                r = requests.post(file_links["commit"], headers=h)
                if r.status_code != 200:
                    continue

            # Add to E-LAUTE community
            if ELAUTE_COMMUNITY_ID:
                r = requests.put(
                    f"{api_url}/{new_record_id}/draft/review",
                    headers=h,
                    data=json.dumps(
                        {
                            "receiver": {"community": ELAUTE_COMMUNITY_ID},
                            "type": "community-submission",
                        }
                    ),
                )

            # Submit the review for the record draft
            r = requests.post(
                f"{api_url}/{record_id}/draft/actions/submit-review",
                headers=h,
            )
            if not r.status_code == 202:
                print(
                    f"Failed to submit review for record {record_id} (code: {r.status_code})"
                )
                failed_updates.append(work_id)

            # Update mapping with new record ID and timestamp
            updated_records.append(
                {
                    "work_id": work_id,
                    "record_id": new_record_id,
                    "file_count": len(file_paths),
                    "created": mapping_row.iloc[0][
                        "created"
                    ],  # Keep original timestamp
                    "updated": current_timestamp,
                }
            )

            print(
                f"Successfully updated record for work_id {work_id}: {new_record_id}"
            )

        except Exception as e:
            print(f"Error updating record for work_id {work_id}: {str(e)}")
            failed_updates.append(work_id)
            continue

    # Update mapping file with new record IDs
    if updated_records:
        updated_df = pd.DataFrame(updated_records)

        # Update existing mapping file by replacing old entries
        for _, updated_row in updated_df.iterrows():
            work_id = updated_row["work_id"]
            existing_mapping.loc[
                existing_mapping["work_id"] == work_id, ["record_id", "updated"]
            ] = [updated_row["record_id"], updated_row["updated"]]

        # Save updated mapping
        existing_mapping.to_csv(mapping_file, index=False, sep=";")
        print(
            f"Updated mapping file with {len(updated_records)} updated records"
        )

    # Summary
    print("\nUPDATE SUMMARY:")
    print(f"   Records checked: {len(work_ids_to_update)}")
    print(f"   Records updated: {len(updated_records)}")
    print(f"   Failed updates: {len(failed_updates)}")

    if updated_records:
        print("\nSuccessfully updated:")
        for record in updated_records:
            print(f"   - {record['work_id']} → {record['record_id']}")

    if failed_updates:
        print("\nFailed to update:")
        for work_id in failed_updates:
            print(f"   - {work_id}")

    return updated_records, failed_updates


def process_work_ids_for_update_or_create():
    """
    Check which work_ids already exist in RDM and split accordingly.
    Create new records for new work_ids and update existing ones if metadata changed.
    """

    # TODO: add get for work_ids and RDM_record_ids via RDM_API and check if update or create

    # Get all work_ids from files that currently are to be uploaded (either created or updated)
    work_ids = get_work_ids_from_files()

    if not work_ids:
        print("No work_ids found.")
        return [], []

    mapping_file = MAPPING_FILE

    # Load existing mapping if it exists
    if os.path.exists(mapping_file):
        existing_mapping = pd.read_csv(mapping_file, sep=";")
        existing_work_ids = set(existing_mapping["work_id"].tolist())
    else:
        existing_work_ids = set()

    # Get work_ids from current files
    current_work_ids = set(work_ids)

    # Split into new and existing work_ids
    new_work_ids = current_work_ids - existing_work_ids
    existing_work_ids_to_check = current_work_ids & existing_work_ids

    return list(new_work_ids), list(existing_work_ids_to_check)


def upload_mei_files(test_one=False):
    """
    Process and upload MEI files to TU RDM grouped by work_id.
    Each work_id becomes one record with multiple files.
    """

    # Get all work_ids
    work_ids = get_work_ids_from_files()

    if not work_ids:
        print("No work_ids found.")
        return

    # Check if we should only process one work_id (for testing)

    upload_one_full = len(sys.argv) > 1 and "--upload-one-full" in sys.argv
    draft_one = len(sys.argv) > 1 and "--draft-one" in sys.argv

    # If --draft-one is set, always process only one work_id
    if draft_one:
        work_ids = work_ids[:1]
        print(
            f"Testing upload with only one work_id (draft mode): {work_ids[0]}"
        )
    elif upload_one_full:
        work_ids = work_ids[:1]
        print(
            f"Testing complete upload with only one work_id (full workflow): {work_ids[0]}"
        )

    # HTTP Headers - following the working sample pattern
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

    api_url = f"{RDM_API_URL}/records"
    api_url_curations = f"{RDM_API_URL}/curations"

    failed_uploads = []
    record_mapping_data = []
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for work_id in work_ids:
        print(f"\n--- Processing work_id: {work_id} ---")

        # Get all files for this work_id
        file_paths = get_files_for_work_id(work_id)

        if not file_paths:
            failed_uploads.append(work_id)
            continue

        try:
            # Combine metadata from all files
            metadata_df, people_df, corporate_df = combine_metadata_for_work_id(
                work_id, file_paths
            )

            if metadata_df.empty:
                failed_uploads.append(work_id)
                continue

            # Create RDM metadata
            metadata = fill_out_basic_metadata_for_work(
                metadata_df, people_df, corporate_df, len(file_paths)
            )

            print(f"Processing {work_id}: {len(file_paths)} files")

            # Save metadata for debugging
            metadata_filename = f"tables/rdm_metadata_{work_id}.json"
            with open(metadata_filename, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            # Create draft record - following working sample pattern
            r = requests.post(api_url, data=json.dumps(metadata), headers=h)
            assert (
                r.status_code == 201
            ), f"Failed to create record (code: {r.status_code})"

            links = r.json()["links"]
            record_id = r.json()["id"]

            # Store the mapping data for CSV
            record_mapping_data.append(
                {
                    "work_id": work_id,
                    "record_id": record_id,
                    "file_count": len(file_paths),
                    "created": current_timestamp,
                    "updated": current_timestamp,
                }
            )

            # Upload each file individually - following working sample pattern
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
            if ELAUTE_COMMUNITY_ID:
                r = requests.put(
                    f"{api_url}/{record_id}/draft/review",
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

                # For production: create curation request and publish
                # Only trigger curation and submit-review if not in --draft-one mode
                if not (len(sys.argv) > 1 and "--draft-one" in sys.argv):
                    r = requests.post(
                        api_url_curations,
                        headers=h,
                        data=json.dumps({"topic": {"record": record_id}}),
                    )
                    assert (
                        r.status_code == 201
                    ), f"Failed to create curation for record {record_id} (code: {r.status_code})"

                    # Submit the review for the record draft
                    r = requests.post(
                        f"{api_url}/{record_id}/draft/actions/submit-review",
                        headers=h,
                    )
                    if not r.status_code == 202:
                        print(
                            f"Failed to submit review for record {record_id} (code: {r.status_code})"
                        )
                        failed_uploads.append(work_id)

        except AssertionError as e:
            print(f"Assertion error processing work_id {work_id}: {str(e)}")
            failed_uploads.append(work_id)
        except Exception as e:
            print(f"Error processing work_id {work_id}: {str(e)}")
            failed_uploads.append(work_id)

    # Save work_id-to-record_id mapping as CSV
    if record_mapping_data:
        mapping_df = pd.DataFrame(record_mapping_data)

        # Use consistent mapping file name and update existing file if it exists
        mapping_file = MAPPING_FILE

        if os.path.exists(mapping_file):
            # Load existing mapping and append new records
            existing_df = pd.read_csv(mapping_file, sep=";")
            combined_df = pd.concat(
                [existing_df, mapping_df], ignore_index=True
            )
            # Remove duplicates, keeping the latest entry for each work_id
            combined_df = combined_df.drop_duplicates(
                subset=["work_id"], keep="last"
            )
            combined_df.to_csv(mapping_file, index=False, sep=";")
        else:
            # Create new mapping file
            mapping_df.to_csv(mapping_file, index=False, sep=";")

        print(mapping_df.head())

    # Summary
    print("\nUPLOAD SUMMARY:")
    print(f"   Failed uploads: {len(failed_uploads)}")

    if failed_uploads:
        print("\nFailed to upload:")
        for failed in failed_uploads:
            print(f"   - {failed}")

    return failed_uploads


def main():
    """
    Main function - choose between testing extraction, uploading files, or updating records.
    """

    # TODO: add check for work_ids and RDM_record_ids via RDM_API and check if update or create

    draft_one = len(sys.argv) > 1 and "--draft-one" in sys.argv
    upload_one_full = len(sys.argv) > 1 and "--upload-one" in sys.argv
    update_records = len(sys.argv) > 1 and "--update" in sys.argv
    update_one = len(sys.argv) > 1 and "--update-one" in sys.argv
    create_urls = len(sys.argv) > 1 and "--create-url-list" in sys.argv

    if len(sys.argv) > 1 and sys.argv[1] == "--upload":
        # Upload mode: process and upload files by work_id
        print("Starting upload process...")
        upload_mei_files()
    elif create_urls:
        # Create URL list from mapping file
        print("Creating URL list from mapping file...")
        create_url_list()
    elif upload_one_full:
        # Upload one work_id with full workflow (curation + publishing)
        print(
            "Testing complete upload process with one work_id (including curation and publishing)..."
        )
        upload_mei_files(test_one=False)
    elif draft_one:
        # Upload one work_id for testing (draft only)
        print("Testing upload process with one work_id (draft only)...")
        upload_mei_files(test_one=True)
    elif update_records:
        # Update mode: check for changes and update existing records
        print("Starting update process for all existing records...")
        new_work_ids, existing_work_ids = (
            process_work_ids_for_update_or_create()
        )

        if existing_work_ids:
            update_records_in_RDM(existing_work_ids)
        else:
            print("No existing records found to update.")

        if new_work_ids:
            print(
                f"\nFound {len(new_work_ids)} new work_ids that could be uploaded with --upload"
            )
    elif update_one:
        # Update one work_id for testing
        print("Testing update process with one existing work_id...")
        new_work_ids, existing_work_ids = (
            process_work_ids_for_update_or_create()
        )

        if existing_work_ids:
            # Just update the first existing work_id
            test_work_ids = existing_work_ids[:1]
            print(f"Testing update with work_id: {test_work_ids[0]}")
            update_records_in_RDM(test_work_ids)
        else:
            print("No existing records found to update.")
    else:
        # Test mode: show work_ids and file groupings
        print("Scanning for work_ids...")
        work_ids = get_work_ids_from_files()

        if not work_ids:
            print("No work_ids found.")
            return

        print(f"Found {len(work_ids)} work_ids:")

        for work_id in work_ids:
            files = get_files_for_work_id(work_id)
            print(f"   - {work_id}: {len(files)} files")
            for file_path in files:
                print(f"     * {os.path.basename(file_path)}")


def create_url_list():
    """Create URL list from mapping file with latest records per work_id"""
    if not os.path.exists(MAPPING_FILE):
        print(f"Mapping file {MAPPING_FILE} not found. Cannot create URL list.")
        return

    # Read the mapping file
    record_df = pd.read_csv(MAPPING_FILE, sep=";")

    # Get only the most recent record_id for each work_id
    latest_records = record_df.loc[
        record_df.groupby("work_id")["updated"].idxmax()
    ]  # HTTP Headers for API requests
    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RDM_API_TOKEN}",
    }

    self_html_list = []
    parent_html_list = []
    failed_records = []

    api_url = f"{RDM_API_URL}/records"

    # Process each record with progress indicator
    print("Fetching URLs from RDM API...")
    for i, r_id in enumerate(latest_records["record_id"], 1):
        print(f"Processing record {i}/{len(latest_records)}: {r_id}")

        try:
            r = requests.get(f"{api_url}/{r_id}", headers=h, timeout=30)

            if r.status_code != 200:
                print(
                    f" Failed to fetch record {r_id} (status: {r.status_code})"
                )
                failed_records.append(r_id)
                parent_html_list.append("")
                self_html_list.append("")
                continue

            response_json = r.json()
            links = response_json.get("links", {})

            parent_html = links.get("parent_html", "")
            self_html = links.get("self_html", "")

            parent_html_list.append(parent_html)
            self_html_list.append(self_html)

        except requests.exceptions.Timeout:
            failed_records.append(r_id)
            parent_html_list.append("")
            self_html_list.append("")
        except Exception:
            failed_records.append(r_id)
            parent_html_list.append("")
            self_html_list.append("")

    # Verify list lengths match
    assert len(parent_html_list) == len(
        latest_records
    ), f"Length mismatch: parent_html_list ({len(parent_html_list)}) vs latest_records ({len(latest_records)})"
    assert len(self_html_list) == len(
        latest_records
    ), f"Length mismatch: self_html_list ({len(self_html_list)}) vs latest_records ({len(latest_records)})"

    latest_records = latest_records.copy()
    latest_records["all_versions_url"] = parent_html_list
    latest_records["current_version_url"] = self_html_list

    # Save the current URL mappings (not historical data)
    latest_records.to_csv(URL_LIST_FILE, index=False, sep=";")

    print(
        f"\nCreated URL list with {len(latest_records)} records: {URL_LIST_FILE}"
    )

    if failed_records:
        print(f"Failed to process {len(failed_records)} records:")
        for failed_id in failed_records:
            print(f"  - {failed_id}")
    else:
        print("All records processed successfully!")


if __name__ == "__main__":
    main()
