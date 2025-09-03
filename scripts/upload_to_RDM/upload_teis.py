from datetime import datetime

import requests

import pandas as pd
import os
import sys
from lxml import etree

import json

from dotenv import load_dotenv

load_dotenv()


# TODO: Configure environement variables so they can be accessed in github


# TODO: where and how to fetch the files from?
# - gitlab repo? (then I need credentials from there as well, I think)


TEI_FILES_PATH = "files"
sources_table = pd.read_csv("tables/filepath_id_lookup.csv")
sources_info_lookup_df = pd.read_excel("tables/sources_table.xlsx")


columns_to_merge = ["Title", "Source_link", "DOI", "RISM_link", "VD_16"]
merged = sources_table.merge(
    sources_info_lookup_df[["ID"] + columns_to_merge],
    left_on="source_id",
    right_on="ID",
    how="left",
)
sources_table = (
    merged.drop(columns=["ID"]) if "ID" in merged.columns else merged
)
# Optionally drop the extra 'source_id' column if not needed
sources_table = (
    merged.drop(columns=["ID"]) if "ID" in merged.columns else merged
)


# Configuration based on TESTING_MODE
TESTING_MODE = True  # Set to False for production


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
    print("🧪 Running in TESTING mode")
    RDM_TOKEN = os.getenv("RDM_TEST_API_TOKEN")
    RDM_API_URL = "https://test.researchdata.tuwien.ac.at/api"
    MAPPING_FILE = "source_id_record_id_mapping_TESTING.csv"
    URL_LIST_FILE = "url_list_TESTING.csv"

    ELAUTE_COMMUNITY_ID = get_id_from_api(
        "https://test.researchdata.tuwien.ac.at/api/communities/e-laute-test"
    )
else:
    print("🚀 Running in PRODUCTION mode")
    RDM_TOKEN = os.getenv("RDM_API_TOKEN")
    RDM_API_URL = "https://researchdata.tuwien.ac.at/api"
    MAPPING_FILE = "source_id_record_id_mapping.csv"
    URL_LIST_FILE = "url_list.csv"

    ELAUTE_COMMUNITY_ID = get_id_from_api(
        "https://researchdata.tuwien.ac.at/api/communities/e-laute"
    )

# see Stackoverflow: https://stackoverflow.com/a/66593457
# needs to be passed in the GitHub Action
# - name: Test env vars for python
#     run: TEST_SECRET=${{ secrets.MY_TOKEN }} python -c 'import os;print(os.environ['TEST_SECRET'])

# RDM_API_URL = os.environ["RDM_API_URL"]
# RDM_API_TOKEN = os.environ["RDM_API_TOKEN"]


# Utility: make HTML link
def make_html_link(url):
    return f'<a href="{url}" target="_blank">{url}</a>'


# Utility: look up source title (stub, replace with actual lookup if needed)
def look_up_source_title(source_id):
    # This should look up the title from a table or database; placeholder:
    title_series = sources_table.loc[
        sources_table["source_id"] == source_id, "Title"
    ]
    if not title_series.empty:
        return title_series.values[0]
    return None


# Utility: look up source links (stub, replace with actual lookup if needed)
def look_up_source_links(source_id):
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


def extract_title_versions(title_elem, ns):
    """Extracts original and regularized versions from a TEI title element."""

    def get_text(node, mode):
        parts = []
        # If node is <choice>, handle orig/reg selection
        if node.tag == f"{{{ns['tei']}}}choice":
            orig = node.find("tei:orig", ns)
            reg = node.find("tei:reg", ns)
            if mode == "orig" and orig is not None:
                parts.append(get_text(orig, mode))
            elif mode == "reg" and reg is not None:
                parts.append(get_text(reg, mode))
            # Also handle tail
            if node.tail:
                parts.append(node.tail)
            return "".join(parts)
        # If node is <lb>, treat as space
        if node.tag == f"{{{ns['tei']}}}lb":
            parts.append(" ")
            if node.tail:
                parts.append(node.tail)
            return "".join(parts)
        # If node is <ex>, just get its text and children
        if node.tag == f"{{{ns['tei']}}}ex":
            if node.text:
                parts.append(node.text)
            for child in node:
                parts.append(get_text(child, mode))
            if node.tail:
                parts.append(node.tail)
            return "".join(parts)
        # For other nodes, get text, then children, then tail
        if node.text:
            parts.append(node.text)
        for child in node:
            parts.append(get_text(child, mode))
        if node.tail:
            parts.append(node.tail)
        return "".join(parts)

    original = get_text(title_elem, "orig").strip()
    regularized = get_text(title_elem, "reg").strip()
    return original, regularized


def get_metadata_df_from_tei(tei_file_path):
    """
    Extract metadata, people, and corporate info from a TEI file (E-LAUTE flavor).
    Returns: (metadata_row, people_df, corporate_df)
    """
    try:
        with open(tei_file_path, "rb") as f:
            content = f.read()

        # Parse XML with TEI namespace
        ns = {"tei": "http://www.tei-c.org/ns/1.0"}
        doc = etree.fromstring(content)

        metadata = {}

        # --- Extract work/source IDs ---
        # Try to get E-LAUTE source ID from altIdentifier[@type='sourceID']/idno
        source_id_elem = doc.find(
            ".//tei:msIdentifier/tei:altIdentifier[@type='sourceID']/tei:idno",
            ns,
        )
        if source_id_elem is not None and source_id_elem.text:
            metadata["source_id"] = source_id_elem.text.strip()
        else:
            metadata["source_id"] = ""

        # Try to get shelfmark
        shelfmark_elem = doc.find(
            ".//tei:msIdentifier/tei:idno[@type='shelfmark']", ns
        )
        if shelfmark_elem is not None and shelfmark_elem.text:
            metadata["shelfmark"] = shelfmark_elem.text.strip()
        else:
            metadata["shelfmark"] = ""

        # --- Extract titles ---
        main_title_elem = doc.find(
            ".//tei:titleStmt/tei:title[@type='main']", ns
        )
        if main_title_elem is not None:
            supplied_elem = main_title_elem.find("tei:supplied", ns)
            if supplied_elem is not None:
                # Get all text inside <supplied>
                supplied_text = "".join(supplied_elem.itertext()).strip()
                metadata["title"] = f"[{supplied_text}]"
                metadata["title_reg"] = ""
            else:
                # Use extract_title_versions to get both original and regularized
                title_orig, title_reg = extract_title_versions(
                    main_title_elem, ns
                )
                metadata["title"] = title_orig
                # Only fill title_reg if different from title
                if (
                    title_reg.strip()
                    and title_reg.strip() != title_orig.strip()
                ):
                    metadata["title_reg"] = title_reg
                else:
                    metadata["title_reg"] = ""
        else:
            # fallback: use msName if available
            msname_elem = doc.find(".//tei:msIdentifier/tei:msName", ns)
            if msname_elem is not None and msname_elem.text:
                metadata["title"] = msname_elem.text.strip()
                metadata["title_reg"] = ""
            else:
                metadata["title"] = ""
                metadata["title_reg"] = ""

        # --- Extract alternative title if present ---
        alt_title_elem = doc.find(
            ".//tei:titleStmt/tei:title[@type='alternative']", ns
        )
        if alt_title_elem is not None:
            alt_title_text = "".join(alt_title_elem.itertext()).strip()
            metadata["title_alternative"] = alt_title_text
        else:
            metadata["title_alternative"] = ""

        print("TITLE ORIGINAL:", metadata["title"])
        print("TITLE REGULARIZED:", metadata["title_reg"])
        print("TITLE ALTERNATIVE:", metadata["title_alternative"])

        # --- Extract source id (use shelfmark if nothing else) ---
        # Try to get from altIdentifier[@type='sourceID']/idno, fallback to shelfmark
        if not metadata["source_id"]:
            metadata["source_id"] = metadata["shelfmark"]

        # --- Extract publication date ---
        pub_date_elem = doc.find(".//tei:publicationStmt/tei:date", ns)
        if pub_date_elem is not None and pub_date_elem.text:
            metadata["publication_date"] = pub_date_elem.text.strip()
        else:
            metadata["publication_date"] = ""

        # --- Extract people (persName) and their roles ---
        people_data = []
        for pers in doc.findall(".//tei:persName", ns):
            role_str = pers.get("role", "")
            # Split roles by whitespace, ignore empty strings
            roles = [r for r in role_str.strip().split() if r]
            ref = pers.get("ref", "")
            xml_id = pers.get("{http://www.w3.org/XML/1998/namespace}id", "")
            forename = pers.find("tei:forename", ns)
            surname = pers.find("tei:surname", ns)
            first_name = (
                forename.text.strip()
                if forename is not None and forename.text
                else ""
            )
            last_name = (
                surname.text.strip()
                if surname is not None and surname.text
                else ""
            )
            full_name = " ".join([first_name, last_name]).strip()
            if not full_name:
                full_name = pers.text.strip() if pers.text else ""
            people_data.append(
                {
                    "role": role_str,  # original role string
                    "roles": roles,  # list of roles
                    "ref": ref,
                    "xml_id": xml_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": full_name,
                }
            )

        # --- Extract corporate entities (orgName) and their roles ---
        corporate_data = []
        for org in doc.findall(".//tei:orgName", ns):
            role = org.get("role", "")
            xml_id = org.get("{http://www.w3.org/XML/1998/namespace}id", "")
            # Try to get label from abbr/expan or text
            abbr = org.find("tei:ref/tei:abbr", ns)
            expan = org.find("tei:ref/tei:expan", ns)
            ref = org.find("tei:ref", ns)
            name = ""
            if abbr is not None and abbr.text:
                name = abbr.text.strip()
            elif expan is not None and expan.text:
                name = expan.text.strip()
            elif ref is not None and ref.text:
                name = ref.text.strip()
            elif org.text:
                name = org.text.strip()
            corporate_data.append(
                {
                    "role": role,
                    "xml_id": xml_id,
                    "name": name,
                }
            )

        # --- License info (licence element) ---
        license_elem = doc.find(
            ".//tei:publicationStmt/tei:availability/tei:licence", ns
        )
        if license_elem is not None:
            metadata["license"] = license_elem.get("target", "")
        else:
            metadata["license"] = ""

        # --- Create DataFrames ---
        people_df = pd.DataFrame(people_data)
        corporate_df = pd.DataFrame(corporate_data)
        metadata_row = pd.DataFrame([metadata])

        return metadata_row, people_df, corporate_df

    except etree.XMLSyntaxError as e:
        print(f"Error parsing TEI file {tei_file_path}: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        print(f"Error processing TEI file {tei_file_path}: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def create_description(row):
    links = look_up_source_links(row["source_id"])
    valid_links = [
        link
        for link in links
        if link is not None and link == link and str(link).strip() != ""
    ]
    links_stringified = ", ".join(make_html_link(link) for link in valid_links)

    # links_stringified = ", ".join(look_up_source_links(row["source_id"]))
    source_id = row["source_id"]
    platform_link = make_html_link(
        f"https://edition.onb.ac.at/fedora/objects/o:lau.{source_id}/methods/sdef:TEI/get"
    )

    title = row["title"]
    subtitle = row.get("title_alternative", "")
    if subtitle and str(subtitle).strip() != "":
        add_titles = f'"{title} - {subtitle}"'
    else:
        add_titles = f'"{title}"'

    part1 = f" <h1>Transcription in TEI of a source from the E-LAUTE project</h1><h2>Overview</h2><p>This dataset contains the transcription of the source {add_titles}, a 16th century lute tablature source, created as part of the E-LAUTE project (<a href=\"https://e-laute.info/\">https://e-laute.info/</a>). The transcription preserves and makes historical lute music and instructions from the German-speaking regions during 1450-1550 accessible.</p><p>The transcription is based on the work with the title \"{row['title']}\" and the id \"{row['source_id']}\" in the e-lautedb.</p>"

    part4 = f"<p>Images of the original source and renderings of the transcriptions can be found on the E-LAUTE platform: {platform_link}.</p>"

    if links_stringified not in [None, ""]:
        part2 = f"<p>Links to the source in other libraries: {links_stringified}.</p>"

    part3 = '<h2>Dataset Contents</h2><p>This dataset consists of one tei-file that contains the transcription of the original source.</p><h2>About the E-LAUTE Project</h2><p><strong>E-LAUTE: Electronic Linked Annotated Unified Tablature Edition - The Lute in the German-Speaking Area 1450-1550</strong></p><p>The E-LAUTE project creates innovative digital editions of lute tablatures from the German-speaking area between 1450 and 1550. This interdisciplinary "open knowledge platform" combines musicology, music practice, music informatics, and literary studies to transform traditional editions into collaborative research spaces.</p><p>For more information, visit the project website: <a href="https://e-laute.info/">https://e-laute.info/</a></p>'

    return part1 + part4 + part2 + part3


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


def fill_out_basic_metadata(metadata_row, people_df, corporate_df):
    """
    Fill out metadata for RDM upload using extracted TEI data.

    Args:
        metadata_row: Single row DataFrame with basic metadata
        people_df: DataFrame with person information
        corporate_df: DataFrame with corporate entity information
    """
    row = metadata_row.iloc[0]  # Get the first (and only) row

    # Build additional_titles list
    additional_titles = []
    # Add alternative title if present
    if row.get("title_alternative"):
        additional_titles.append(
            {
                "lang": {"id": "deu", "title": {"en": "German"}},
                "title": row["title_alternative"],
                "type": {
                    "id": "alternative-title",
                    "title": {"en": "Alternative title"},
                },
            }
        )
    # Add regularized title if present and different from main title
    if (
        row.get("title_reg")
        and row["title_reg"].strip()
        and row["title_reg"].strip() != row["title"].strip()
    ):
        additional_titles.append(
            {
                "lang": {"id": "deu", "title": {"en": "German"}},
                "title": row["title_reg"],
                "type": {
                    "id": "alternative-title",
                    "title": {"en": "Alternative title"},
                },
            }
        )
    metadata = {
        "metadata": {
            "title": f'{row["title"]} ({row["source_id"]}) TEI Transcription',
            "additional_titles": additional_titles,
            # "additional_titles": [
            #     {
            #         "lang": {
            #             "id": "gmh",
            #             "title": {"en": "Middle High German (ca. 1050-1500)"},
            #         },
            #         "title": row["title"],
            #         "type": {
            #             "id": "alternative-title",
            #             "title": {"en": "Alternative title"},
            #         },
            #     },
            #     (
            #         (
            #             {
            #                 "lang": {"id": "deu", "title": {"en": "German"}},
            #                 "title": row["translated_title"],
            #                 "type": {
            #                     "id": "translated-title",
            #                     "title": {"en": "Translated title"},
            #                 },
            #             },
            #         )
            #         if row["translated_title"]
            #         else None
            #     ),
            # ],
            "creators": [],
            "contributors": [],
            "description": create_description(row),
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
            "subjects": [
                {"subject": "16th century"},
                {
                    "id": "http://www.oecd.org/science/inno/38235147.pdf?6.4",
                    "scheme": "FOS",
                    "subject": "Arts (arts, history of arts, performing arts, music)",
                },
                {"subject": "lute music"},
                {"subject": "lute instructions"},
            ],
        }
    }

    # Add people as creators and contributors based on their roles
    # Track people who have been added as creators to avoid duplicates
    creator_names = set()

    # First pass: Add editors as creators
    for _, person in people_df.iterrows():
        if (
            person.get("role") == "editor"
            or person.get("role") == "editor metadataContact"
        ):
            person_entry = {
                "person_or_org": {
                    "family_name": person.get("last_name", ""),
                    "given_name": person.get("first_name", ""),
                    "name": person.get("full_name", ""),
                    "type": "personal",
                }
            }

            person_entry["role"] = {"id": "editor", "title": {"en": "Editor"}}
            metadata["metadata"]["creators"].append(person_entry)
            creator_names.add(person.get("full_name", ""))

    # Second pass: Add people both editor and metadata contacts as contributors
    for _, person in people_df.iterrows():
        if (
            person.get("role") == "editor metadataContact"
            or person.get("role") == "metadataContact"
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
                "id": "contactperson",
                "title": {"en": "Contact person"},
            }
            metadata["metadata"]["contributors"].append(person_entry)
            creator_names.add(person.get("full_name", ""))

    # Third pass: Add all other roles as contributors
    for _, person in people_df.iterrows():
        if person.get("role") not in [
            "editor",
            "metadataContact",
            "editor metadataContact",
        ]:
            person_entry = {
                "person_or_org": {
                    "family_name": person.get("last_name", ""),
                    "given_name": person.get("first_name", ""),
                    "name": person.get("full_name", ""),
                    "type": "personal",
                }
            }

            # Map TEI roles to RDM roles
            role_mapping = {
                "teiEditor": {"id": "editor", "title": {"en": "Editor"}},
                "metadataContact": {
                    "id": "contactperson",
                    "title": {"en": "Contact person"},
                },
                "publisher": {"id": "other", "title": {"en": "Publisher"}},
                "HeadOfSubproject": {
                    "id": "workpackageleader",
                    "title": {"en": "Work package leader"},
                },
            }

            person_entry["role"] = role_mapping.get(
                person.get("role", ""),
                {"id": "other", "title": {"en": "Other"}},
            )

            metadata["metadata"]["contributors"].append(person_entry)

    # Add source links as related identifiers
    links_to_source = look_up_source_links(row["source_id"])
    # Filter out empty, None, or NaN links
    links_to_source = [
        link
        for link in links_to_source
        if link and str(link).strip() != "" and link == link
    ]
    if links_to_source:
        metadata["metadata"]["related_identifiers"].extend(
            create_related_identifiers(links_to_source)
        )

    return metadata


def get_metadata_for_source(source_id, file_path):
    """
    Extract metadata, people, and corporate info for a single source_id and file_path.
    """
    metadata_df, people_df, corporate_df = get_metadata_df_from_tei(file_path)
    return metadata_df, people_df, corporate_df


def update_records_in_RDM(source_ids_to_update):
    """Update existing records in RDM if metadata has changed. Only report files that did not reach and pass submit-review."""

    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RDM_TOKEN}",
    }
    fh = {
        "Accept": "application/json",
        "Content-Type": "application/octet-stream",
        "Authorization": f"Bearer {RDM_TOKEN}",
    }

    mapping_file = MAPPING_FILE
    if not os.path.exists(mapping_file):
        print(f"Mapping file {mapping_file} not found. No records to update.")
        return

    existing_mapping = pd.read_csv(mapping_file, sep=";")
    failed_uploads = []

    for source_id in source_ids_to_update:
        print(f"\n--- Checking for updates: {source_id} ---")
        mapping_row = existing_mapping[
            existing_mapping["source_id"] == source_id
        ]
        if mapping_row.empty:
            print(
                f"Source ID {source_id} not found in existing records. Skipping."
            )
            continue

        record_id = mapping_row.iloc[0]["record_id"]

        try:
            file_row = sources_table[sources_table["source_id"] == source_id]
            if file_row.empty:
                print(f"No file found for source_id: {source_id}")
                failed_uploads.append(source_id)
                continue
            file_path = file_row.iloc[0]["file_path"]

            metadata_df, people_df, corporate_df = get_metadata_for_source(
                source_id, file_path
            )
            if metadata_df.empty:
                print(f"Failed to extract metadata for source_id {source_id}")
                failed_uploads.append(source_id)
                continue

            new_metadata_structure = fill_out_basic_metadata(
                metadata_df, people_df, corporate_df
            )
            new_metadata = new_metadata_structure["metadata"]

            r = requests.get(f"{RDM_API_URL}/records/{record_id}", headers=h)
            if r.status_code != 200:
                print(
                    f"Failed to fetch record {record_id} (code: {r.status_code})"
                )
                failed_uploads.append(source_id)
                continue

            current_record = r.json()
            current_metadata = current_record.get("metadata", {})

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
                f"Metadata changes detected for source_id {source_id} in fields: {', '.join(changes_detected)}"
            )

            # Create a new version/draft for the record
            r = requests.post(
                f"{RDM_API_URL}/records/{record_id}/versions", headers=h
            )
            if r.status_code != 201:
                print(
                    f"Failed to create new version for record {record_id} (code: {r.status_code})"
                )
                failed_uploads.append(source_id)
                continue

            new_version_data = r.json()
            new_record_id = new_version_data["id"]

            # Update the draft with new metadata
            r = requests.put(
                f"{RDM_API_URL}/records/{new_record_id}/draft",
                data=json.dumps(new_metadata_structure),
                headers=h,
            )
            if r.status_code != 200:
                print(
                    f"Failed to update draft {new_record_id} (code: {r.status_code})"
                )
                failed_uploads.append(source_id)
                continue

            # Update files - delete existing and upload new ones
            r = requests.delete(
                f"{RDM_API_URL}/records/{new_record_id}/draft/files", headers=h
            )

            # Upload files
            file_entries = []
            for file_path in file_path:
                file_entries.append({"key": os.path.basename(file_path)})

            # Initialize files
            data = json.dumps(file_entries)
            r = requests.post(
                f"{RDM_API_URL}/records/{new_record_id}/draft/files",
                data=data,
                headers=h,
            )
            if r.status_code != 201:
                print(
                    f"Failed to initialize files for record {new_record_id} (code: {r.status_code})"
                )
                failed_uploads.append(source_id)
                continue

            file_responses = r.json()["entries"]

            # Upload each file
            for i, file_path in enumerate(file_path):
                file_links = file_responses[i]["links"]

                # Upload file content
                with open(file_path, "rb") as fp:
                    r = requests.put(file_links["content"], data=fp, headers=fh)
                if r.status_code != 200:
                    failed_uploads.append(source_id)
                    continue

                # Commit the file
                r = requests.post(file_links["commit"], headers=h)
                if r.status_code != 200:
                    failed_uploads.append(source_id)
                    continue

            # Add to E-LAUTE community
            if ELAUTE_COMMUNITY_ID:
                r = requests.put(
                    f"{RDM_API_URL}/records/{new_record_id}/draft/review",
                    headers=h,
                    data=json.dumps(
                        {
                            "receiver": {"community": ELAUTE_COMMUNITY_ID},
                            "type": "community-submission",
                        }
                    ),
                )

            # Publish the updated record
            r = requests.post(
                f"{RDM_API_URL}/records/{new_record_id}/draft/actions/submit-review",
                headers=h,
            )
            if not r.status_code == 202:
                print(
                    f"Failed to submit review for record {record_id} (code: {r.status_code})"
                )
                failed_uploads.append(source_id)

        except Exception as e:
            print(f"Error updating record for source_id {source_id}: {str(e)}")
            failed_uploads.append(source_id)
            continue

    print("\nFAILED UPLOADS (did not reach/pass submit-review):")
    for source_id in failed_uploads:
        print(f"   - {source_id}")

    return failed_uploads


def process_source_ids_for_update_or_create():
    """
    Check which source_ids already exist in RDM and split accordingly.
    Create new records for new source_ids and update existing ones if metadata changed.
    """

    # Get all source_ids from files
    source_ids = sources_table["source_id"].tolist()

    if not source_ids:
        print("No source ids found.")
        return [], []

    mapping_file = MAPPING_FILE

    # Load existing mapping if it exists
    if os.path.exists(mapping_file):
        existing_mapping = pd.read_csv(mapping_file, sep=";")
        existing_source_ids = set(existing_mapping["source_id"].tolist())
    else:
        existing_source_ids = set()

    # Get source_ids from current files
    current_source_ids = set(source_ids)

    # Split into new and existing source_ids
    new_source_ids = current_source_ids - existing_source_ids
    existing_source_ids_to_check = current_source_ids & existing_source_ids

    return list(new_source_ids), list(existing_source_ids_to_check)


def upload_tei_files(test_one=False, test_all=False):
    """
    Process and upload TEI files to TU RDM, one record per source_id/file_path.
    """
    # Get all sources
    sources = sources_table[["source_id", "file_path"]].to_dict(
        orient="records"
    )

    if not sources:
        print("No sources found.")
        return

    # Check if we should only process one source (for testing)
    upload_one_full = len(sys.argv) > 1 and "--upload-one-full" in sys.argv
    upload_one = len(sys.argv) > 1 and "--upload-one" in sys.argv

    if upload_one or upload_one_full:
        sources = sources[:1]
        if upload_one:
            print(
                f"Testing upload with only one source (draft mode): {sources[0]['source_id']}"
            )
        elif upload_one_full:
            print(
                f"Testing complete upload with only one source (full workflow): {sources[0]['source_id']}"
            )
    # HTTP Headers
    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RDM_TOKEN}",
    }

    fh = {
        "Accept": "application/json",
        "Content-Type": "application/octet-stream",
        "Authorization": f"Bearer {RDM_TOKEN}",
    }

    api_url = f"{RDM_API_URL}/records"
    api_url_curations = f"{RDM_API_URL}/curations"

    successful_uploads = []
    failed_uploads = []
    record_mapping_data = []
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for source in sources:
        source_id = source["source_id"]
        file_path = source["file_path"]
        print(f"\n--- Processing source_id: {source_id} ---")

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            failed_uploads.append(source_id)
            continue

        try:
            # Extract metadata from the file
            metadata_df, people_df, corporate_df = get_metadata_for_source(
                source_id, file_path
            )

            if metadata_df.empty:
                failed_uploads.append(source_id)
                continue

            # Create RDM metadata
            metadata = fill_out_basic_metadata(
                metadata_df, people_df, corporate_df
            )

            print(f"Processing {source_id}: {file_path}")

            # Create draft record
            r = requests.post(api_url, data=json.dumps(metadata), headers=h)
            assert (
                r.status_code == 201
            ), f"Failed to create record (code: {r.status_code})"

            links = r.json()["links"]
            record_id = r.json()["id"]

            # Store the mapping data for CSV
            record_mapping_data.append(
                {
                    "source_id": source_id,
                    "record_id": record_id,
                    "file_count": 1,
                    "created": current_timestamp,
                    "updated": current_timestamp,
                }
            )

            # Upload the file
            filename = os.path.basename(file_path)
            data = json.dumps([{"key": filename}])
            r = requests.post(links["files"], data=data, headers=h)
            assert (
                r.status_code == 201
            ), f"Failed to create file {filename} (code: {r.status_code})"

            file_links = r.json()["entries"][0]["links"]

            # Upload file content
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
                failed_uploads.append(source_id)

        except AssertionError as e:
            print(f"Assertion error processing source_id {source_id}: {str(e)}")
            failed_uploads.append(source_id)
        except Exception as e:
            print(f"Error processing source_id {source_id}: {str(e)}")
            failed_uploads.append(source_id)

    # Save source_id-to-record_id mapping as CSV
    if record_mapping_data:
        mapping_df = pd.DataFrame(record_mapping_data)
        mapping_file = MAPPING_FILE
        if os.path.exists(mapping_file):
            existing_df = pd.read_csv(mapping_file, sep=";")
            combined_df = pd.concat(
                [existing_df, mapping_df], ignore_index=True
            )
            combined_df = combined_df.drop_duplicates(
                subset=["source_id"], keep="last"
            )
            combined_df.to_csv(mapping_file, index=False, sep=";")
        else:
            mapping_df.to_csv(mapping_file, index=False, sep=";")
        print(mapping_df.head())

        # Summary

        print("\nUPLOAD SUMMARY:")
        print(f"   Successful uploads: {len(successful_uploads)}")
        print(f"   Failed uploads: {len(failed_uploads)}")
        if successful_uploads:
            print("\nSuccessfully uploaded:")
            for upload in successful_uploads:
                status = upload.get("status", "unknown")
                print(
                    f"   - {upload['source_id']} → {upload['record_id']} (1 file) - {status}"
                )
        if failed_uploads:
            print("\nFailed to upload:")
            for failed in failed_uploads:
                print(f"   - {failed}")

    return successful_uploads, failed_uploads


def main():
    """
    Main function - choose between testing extraction, uploading files, or updating records.
    """
    # Check for flags
    upload_one = len(sys.argv) > 1 and "--draft-one" in sys.argv
    upload_one_full = len(sys.argv) > 1 and "--upload-one" in sys.argv
    update_records = len(sys.argv) > 1 and "--update" in sys.argv
    update_one = len(sys.argv) > 1 and "--update-one" in sys.argv
    create_urls = len(sys.argv) > 1 and "--create-url-list" in sys.argv

    if len(sys.argv) > 1 and sys.argv[1] == "--upload":
        print("Starting upload process...")
        upload_tei_files()
    elif upload_one_full:
        print(
            "Testing complete upload process with one source_id (including curation and publishing)..."
        )
        upload_tei_files(test_one=False)
    elif upload_one:
        print("Testing upload process with one source_id (draft only)...")
        upload_tei_files(test_one=True)
    elif create_urls:
        print("Creating URL list from mapping file...")
        create_url_list()
    elif update_records:
        print("Starting update process for all existing records...")
        new_source_ids, existing_source_ids = (
            process_source_ids_for_update_or_create()
        )

        if existing_source_ids:
            update_records_in_RDM(existing_source_ids)
        else:
            print("No existing records found to update.")

        if new_source_ids:
            print(
                f"\nFound {len(new_source_ids)} new source_ids that could be uploaded with --upload"
            )
    elif update_one:
        print("Testing update process with one existing source_id...")
        new_source_ids, existing_source_ids = (
            process_source_ids_for_update_or_create()
        )

        if existing_source_ids:
            test_source_ids = existing_source_ids[:1]
            print(f"Testing update with source_id: {test_source_ids[0]}")
            update_records_in_RDM(test_source_ids)
        else:
            print("No existing records found to update.")
    else:
        print("Scanning for source_ids...")
        sources = sources_table[["source_id", "file_path"]].to_dict(
            orient="records"
        )
        if not sources:
            print("No sources found.")
            return
        print(f"Found {len(sources)} source_ids:")
        for source in sources:
            print(
                f"   - {source['source_id']}: {os.path.basename(source['file_path'])}"
            )


def create_url_list():
    """Create URL list from mapping file with latest records per source_id"""
    if not os.path.exists(MAPPING_FILE):
        print(f"Mapping file {MAPPING_FILE} not found. Cannot create URL list.")
        return

    # Read the mapping file
    record_df = pd.read_csv(MAPPING_FILE, sep=";")

    # Get only the most recent record_id for each source_id
    latest_records = record_df.loc[
        record_df.groupby("source_id")["updated"].idxmax()
    ]  # HTTP Headers for API requests
    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RDM_TOKEN}",
    }

    self_html_list = []
    parent_html_list = []
    failed_records = []

    # Process each record with progress indicator
    print("Fetching URLs from RDM API...")
    for i, r_id in enumerate(latest_records["record_id"], 1):
        print(f"Processing record {i}/{len(latest_records)}: {r_id}")

        try:
            r = requests.get(
                f"{RDM_API_URL}/records/{r_id}", headers=h, timeout=30
            )

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
