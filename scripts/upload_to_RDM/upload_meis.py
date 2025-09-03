import pandas as pd
import os
import sys
from lxml import etree

import requests

import re

from datetime import datetime


from . import rdm_upload_utils


(
    RDM_API_URL,
    RDM_API_TOKEN,
    FILES_PATH,
    ELAUTE_COMMUNITY_ID,
) = rdm_upload_utils.setup_for_rdm_api_access(TESTING_MODE=True, GA_MODE=False)


errors = []
metadata_df = pd.DataFrame()
sources_table = pd.DataFrame()

# TODO: implement extraction of info about sources from knowledge graph/dbrepo and not from exel-file
sources_excel_df = pd.read_excel(
    "scripts/upload_to_RDM/tables/sources_table.xlsx"
)
sources_table["source_id"] = sources_excel_df["ID"].fillna(
    sources_excel_df["Shelfmark"]
)
sources_table["Title"] = sources_excel_df["Title"]
sources_table["Source_link"] = sources_excel_df["Source_link"].fillna("")
sources_table["RISM_link"] = sources_excel_df["RISM_link"].fillna("")
sources_table["VD_16"] = sources_excel_df["VD_16"].fillna("")


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


def get_work_ids_from_files():
    """
    Scan all folders in FILES_PATH and extract unique work_ids.
    Extract everything up to and including the 'n' + number part.
    Example: Jud_1523-2_n10_18v_enc_dipl_GLT.mei -> Jud_1523-2_n10
    """
    work_ids = set()

    print(FILES_PATH)

    for root, dirs, files in os.walk(FILES_PATH):
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

    for root, dirs, files in os.walk(FILES_PATH):
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
    links = rdm_upload_utils.look_up_source_links(
        sources_table, row["source_id"]
    )
    for link in links if links else []:
        links_stringified += rdm_upload_utils.make_html_link(link) + ", "

    source_id = row["source_id"]
    work_number = row["work_id"].split("_")[-1]
    platform_link = rdm_upload_utils.make_html_link(
        f"https://edition.onb.ac.at/fedora/objects/o:lau.{source_id}/methods/sdef:TEI/get?mode={work_number}"
    )

    part1 = f"<h1>Transcriptions in MEI of a lute piece from the E-LAUTE project</h1><h2>Overview</h2><p>This dataset contains transcription files of the piece \"{row['title']}\", a 16th century lute music piece originally notated in lute tablature, created as part of the E-LAUTE project ({rdm_upload_utils.make_html_link('https://e-laute.info/')}). The transcriptions preserve and make historical lute music from the German-speaking regions during 1450-1550 accessible.</p><p>They are based on the work with the title \"{row['title']}\" and the id \"{row['work_id']}\" in the e-lautedb. It is found on the page(s) or folio(s) {row['fol_or_p']} in the source \"{rdm_upload_utils.look_up_source_title(sources_table, row['source_id'])}\" with the E-LAUTE source-id \"{row['source_id']}\" and the shelfmark {row['shelfmark']}.</p>"

    part4 = f"<p>Images of the original source and renderings of the transcriptions can be found on the E-LAUTE platform: {platform_link}.</p>"

    if links_stringified not in [None, ""]:
        part2 = f"<p>Links to the source: {links_stringified}.</p>"
    else:
        part2 = ""

    part3 = f'<h2>Dataset Contents</h2><p>This dataset includes {file_count} MEI files with different transcription variants (diplomatic/editorial versions in various tablature notations and common music notation).</p><h2>About the E-LAUTE Project</h2><p><strong>E-LAUTE: Electronic Linked Annotated Unified Tablature Edition - The Lute in the German-Speaking Area 1450-1550</strong></p><p>The E-LAUTE project creates innovative digital editions of lute tablatures from the German-speaking area between 1450 and 1550. This interdisciplinary "open knowledge platform" combines musicology, music practice, music informatics, and literary studies to transform traditional editions into collaborative research spaces.</p><p>For more information, visit the project website: {rdm_upload_utils.make_html_link('https://e-laute.info/')}</p>'

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
            "identifiers": [
                {"identifier": f"{row['work_id']}", "scheme": "other"}
            ],
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

    # Third pass: Add meiEditors and fronimoEditors as creators
    for _, person in people_df.iterrows():
        if (
            person.get("role") in ["meiEditor", "fronimoEditor"]
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
            person_entry["role"] = {"id": "editor", "title": {"en": "Editor"}}
            metadata["metadata"]["creators"].append(person_entry)
            creator_names.add(person.get("full_name", ""))

    # Fourth pass: Add all other roles as contributors (excluding those already added as creators)
    for _, person in people_df.iterrows():
        if person.get("role") not in [
            "author",
            "intabulator",
            "meiEditor",
            "fronimoEditor",
        ]:
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
    links_to_source = rdm_upload_utils.look_up_source_links(
        sources_table, row["source_id"]
    )
    if links_to_source:
        metadata["metadata"]["related_identifiers"].extend(
            rdm_upload_utils.create_related_identifiers(links_to_source)
        )

    return metadata


def update_records_in_RDM(work_ids_to_update, draft_one=False):
    """Update existing records in RDM if metadata has changed."""

    # HTTP Headers
    h, fh = rdm_upload_utils.set_headers(RDM_API_TOKEN)

    # Load existing work_id to record_id mapping
    existing_records = rdm_upload_utils.get_records_from_RDM(
        RDM_API_TOKEN, RDM_API_URL, ELAUTE_COMMUNITY_ID
    )

    # current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated_records = []
    failed_updates = []

    for work_id in work_ids_to_update:
        print(f"\n--- Checking for updates: {work_id} ---")

        # Check if work_id exists in mapping
        mapping_row = existing_records[existing_records["elaute_id"] == work_id]
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
            r = requests.get(f"{RDM_API_URL}/records/{record_id}", headers=h)
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
                "identifiers",
                "dates",
                "publisher",
                "references",
                "related_identifiers",
                "resource_type",
                "rights",
            ]

            # Check for metadata changes
            metadata_changed = False
            changes_detected = []

            for field in fields_to_compare:
                current_value = current_metadata.get(field)
                new_value = new_metadata.get(field)

                if not rdm_upload_utils.deep_compare_metadata(
                    current_value, new_value
                ):
                    metadata_changed = True
                    changes_detected.append(field)

            if not metadata_changed:
                continue

            print(
                f"Metadata changes detected for work_id {work_id} in fields: {', '.join(changes_detected)}"
            )

            # --- UPLOAD ---

            fails = rdm_upload_utils.upload_v2(
                metadata=new_metadata,
                elaute_id=work_id,
                file_paths=file_paths,
                RDM_API_TOKEN=RDM_API_TOKEN,
                RDM_API_URL=RDM_API_URL,
                ELAUTE_COMMUNITY_ID=ELAUTE_COMMUNITY_ID,
                draft_one=draft_one,
            )
            failed_updates.extend(fails)

        except Exception as e:
            print(f"Error uploading files for work_id {work_id}: {e}")

        #     # Create a new version/draft for the record
        #     r = requests.post(f"{api_url}/{record_id}/versions", headers=h)
        #     if r.status_code != 201:
        #         print(
        #             f"Failed to create new version for record {record_id} (code: {r.status_code})"
        #         )
        #         failed_updates.append(work_id)
        #         continue

        #     new_version_data = r.json()
        #     new_record_id = new_version_data["id"]

        #     # Update the draft with new metadata
        #     r = requests.put(
        #         f"{api_url}/{new_record_id}/draft",
        #         data=json.dumps(new_metadata_structure),
        #         headers=h,
        #     )
        #     if r.status_code != 200:
        #         print(
        #             f"Failed to update draft {new_record_id} (code: {r.status_code})"
        #         )
        #         failed_updates.append(work_id)
        #         continue

        #     # Update files - delete existing and upload new ones
        #     r = requests.delete(
        #         f"{api_url}/{new_record_id}/draft/files", headers=h
        #     )

        #     # Upload files
        #     file_entries = []
        #     for file_path in file_paths:
        #         file_entries.append({"key": os.path.basename(file_path)})

        #     # Initialize files
        #     data = json.dumps(file_entries)
        #     r = requests.post(
        #         f"{api_url}/{new_record_id}/draft/files",
        #         data=data,
        #         headers=h,
        #     )
        #     if r.status_code != 201:
        #         print(
        #             f"Failed to initialize files for record {new_record_id} (code: {r.status_code})"
        #         )
        #         failed_updates.append(work_id)
        #         continue

        #     file_responses = r.json()["entries"]

        #     # Upload each file
        #     for i, file_path in enumerate(file_paths):
        #         file_links = file_responses[i]["links"]

        #         # Upload file content
        #         with open(file_path, "rb") as fp:
        #             r = requests.put(file_links["content"], data=fp, headers=fh)
        #         if r.status_code != 200:
        #             continue

        #         # Commit the file
        #         r = requests.post(file_links["commit"], headers=h)
        #         if r.status_code != 200:
        #             continue

        #     # Add to E-LAUTE community
        #     if ELAUTE_COMMUNITY_ID:
        #         r = requests.put(
        #             f"{api_url}/{new_record_id}/draft/review",
        #             headers=h,
        #             data=json.dumps(
        #                 {
        #                     "receiver": {"community": ELAUTE_COMMUNITY_ID},
        #                     "type": "community-submission",
        #                 }
        #             ),
        #         )

        #     # Submit the review for the record draft
        #     r = requests.post(
        #         f"{api_url}/{record_id}/draft/actions/submit-review",
        #         headers=h,
        #     )
        #     if not r.status_code == 202:
        #         print(
        #             f"Failed to submit review for record {record_id} (code: {r.status_code})"
        #         )
        #         failed_updates.append(work_id)

        #     # Update mapping with new record ID and timestamp
        #     updated_records.append(
        #         {
        #             "work_id": work_id,
        #             "record_id": new_record_id,
        #             "file_count": len(file_paths),
        #             "created": mapping_row.iloc[0][
        #                 "created"
        #             ],  # Keep original timestamp
        #             "updated": current_timestamp,
        #         }
        #     )

        #     print(
        #         f"Successfully updated record for work_id {work_id}: {new_record_id}"
        #     )

        # except Exception as e:
        #     print(f"Error updating record for work_id {work_id}: {str(e)}")
        #     failed_updates.append(work_id)
        #     continue

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

    # Get all work_ids from files that currently are to be uploaded (either created or updated)
    work_ids = get_work_ids_from_files()

    if not work_ids:
        print("No work_ids found.")
        return [], []

    existing_records = rdm_upload_utils.get_records_from_RDM(
        RDM_API_TOKEN, RDM_API_URL, ELAUTE_COMMUNITY_ID
    )

    print(type(existing_records))
    print(existing_records)

    existing_work_ids = set(existing_records["elaute_id"].tolist())

    # Get work_ids from current files
    current_work_ids = set(work_ids)

    # Split into new and existing work_ids
    new_work_ids = current_work_ids - existing_work_ids
    existing_work_ids_to_check = current_work_ids & existing_work_ids

    return list(new_work_ids), list(existing_work_ids_to_check)


def upload_mei_files(work_ids, draft_one=False):
    """
    Process and upload MEI files to TU RDM grouped by work_id.
    Each work_id becomes one record with multiple files.
    """
    failed_uploads = []

    if not work_ids:
        print("No work_ids found.")
        return

    # Check if we should only process one work_id (for testing)

    # If --draft-one is set, always process only one work_id
    if draft_one:
        work_ids = work_ids[:1]
        print(
            f"Testing upload with only one work_id (draft mode): {work_ids[0]}"
        )

    # HTTP Headers
    h, fh = rdm_upload_utils.set_headers(RDM_API_TOKEN)

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

            # ---- UPLOAD ---

            fails = rdm_upload_utils.upload_v2(
                metadata=metadata,
                elaute_id=work_id,
                file_paths=file_paths,
                RDM_API_TOKEN=RDM_API_TOKEN,
                RDM_API_URL=RDM_API_URL,
                ELAUTE_COMMUNITY_ID=ELAUTE_COMMUNITY_ID,
                draft_one=draft_one,
            )
            failed_uploads.extend(fails)

        except AssertionError as e:
            print(f"Assertion error processing work_id {work_id}: {str(e)}")
            failed_uploads.append(work_id)
        except Exception as e:
            print(f"Error processing work_id {work_id}: {str(e)}")
            failed_uploads.append(work_id)
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

    new_work_ids, existing_work_ids = process_work_ids_for_update_or_create()

    if len(new_work_ids) > 0:
        upload_mei_files(new_work_ids, draft_one)

    if len(existing_work_ids) > 0:
        update_records_in_RDM(existing_work_ids, draft_one)


if __name__ == "__main__":
    main()
