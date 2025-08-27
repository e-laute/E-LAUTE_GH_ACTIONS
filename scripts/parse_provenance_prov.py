#!/usr/bin/env python3
"""
Extract MEI header metadata and convert to PROV-O RDF in Turtle format.

Usage:
    python parse_provenance_prov.py <mei_file> > provenance.ttl

This script extracts metadata from MEI headers and generates PROV-O compliant
RDF statements that can be ingested into a graph database.
"""
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from datetime import datetime
import re

# Official MEI namespace used in MEI 5.x files
MEI_NS = {"mei": "http://www.music-encoding.org/ns/mei"}

# PROV-O and related namespaces
PROV_NS = {
    "prov": "http://www.w3.org/ns/prov#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "bibo": "http://purl.org/ontology/bibo/",
    "mei": "http://www.music-encoding.org/ns/mei#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "loc": "http://id.loc.gov/vocabulary/relators/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "elaute": "https://e-laute.info/vocab#"
}

# Set of E-LAUTE specific roles that should use E-LAUTE namespace
ELAUTE_ROLE_MAPPING = {
    "meiEditor",  # editor
    "fronimoEditor",  # editor
    "musescoreEditor",  # editor
    "metadataContact",  # contributor
    "intabulator",  # arranger
    "provider",  # publisher
    "funder",  # funder
    "publisher",  # publisher
    "author",  # author
    "composer",  # composer
    "scribe",  # scribe
    "collector",  # collector
}

def _get_role_uri(role: str) -> str:
    """Convert a role to its proper URI, using E-LAUTE URIs for E-LAUTE specific roles."""
    if role in ELAUTE_ROLE_MAPPING:
        # E-LAUTE specific role - use E-LAUTE URI
        return f"elaute:{role}"
    elif role in [
        "abridger", "actor", "adapter", "addressee", "analyst", "animator", "annotator",
        "announcer", "appellant", "appellee", "applicant", "architect", "arranger",
        "art copyist", "art director", "artist", "artistic director", "assignee",
        "associated name", "attributed name", "auctioneer", "audio engineer",
        "audio producer", "author", "author in quotations or text abstracts",
        "author of afterword, colophon, etc.", "author of dialog",
        "author of introduction, etc.", "autographer", "bibliographic antecedent",
        "binder", "binding designer", "blurb writer", "book artist", "book designer",
        "book producer", "bookjacket designer", "bookplate designer", "bookseller",
        "braille embosser", "broadcaster", "calligrapher", "camera operator",
        "cartographer", "caster", "casting director", "censor", "choreographer",
        "cinematographer", "client", "collection registrar", "collector", "collotyper",
        "colorist", "commentator", "commentator for written text", "compiler",
        "complainant", "complainant-appellant", "complainant-appellee", "composer",
        "compositor", "conceptor", "conductor", "conservator", "consultant",
        "consultant to a project", "contestant", "contestant-appellant",
        "contestant-appellee", "contestee", "contestee-appellant", "contestee-appellee",
        "contractor", "contributor", "copyright claimant", "copyright holder",
        "corrector", "correspondent", "costume designer", "court governed",
        "court reporter", "cover designer", "creator", "curator", "dancer",
        "data contributor", "data manager", "dedicatee", "dedicator", "defendant",
        "defendant-appellant", "defendant-appellee", "degree committee member",
        "degree granting institution", "degree supervisor", "delineator", "depicted",
        "depositor", "designer", "director", "dissertant", "distribution place",
        "distributor", "dj", "donor", "draftsman", "dubbing director", "dubious author",
        "editor", "editor of compilation", "editor of moving image work",
        "editorial director", "electrician", "electrotyper", "enacting jurisdiction",
        "engineer", "engraver", "etcher", "event place", "expert", "facsimilist",
        "field director", "film director", "film distributor", "film editor",
        "film producer", "filmmaker", "first party", "forger", "former owner",
        "founder", "funder", "game developer", "geographic information specialist",
        "honoree", "host", "host institution", "illuminator", "illustrator",
        "inker", "inscriber", "instrumentalist", "interviewee", "interviewer",
        "inventor", "issuing body", "judge", "jurisdiction governed", "laboratory",
        "laboratory director", "landscape architect", "lead", "lender", "letterer",
        "libelant", "libelant-appellant", "libelant-appellee", "libelee",
        "libelee-appellant", "libelee-appellee", "librettist", "licensee",
        "licensor", "lighting designer", "lithographer", "lyricist", "makeup artist",
        "manufacture place", "manufacturer", "marbler", "markup editor", "medium",
        "metadata contact", "metal engraver", "minute taker", "mixing engineer",
        "moderator", "monitor", "music copyist", "music programmer", "musical director",
        "musician", "narrator", "news anchor", "onscreen participant",
        "onscreen presenter", "opponent", "organizer", "originator", "other",
        "owner", "panelist", "papermaker", "patent applicant", "patent holder",
        "patron", "penciller", "performer", "permitting agency", "photographer",
        "place of address", "plaintiff", "plaintiff-appellant", "plaintiff-appellee",
        "platemaker", "praeses", "presenter", "printer", "printer of plates",
        "printmaker", "process contact", "producer", "production company",
        "production designer", "production manager", "production personnel",
        "production place", "programmer", "project director", "proofreader",
        "provider", "publication place", "publisher", "publisher director",
        "puppeteer", "radio director", "radio producer", "rapporteur",
        "recording engineer", "recordist", "redaktor", "remix artist", "renderer",
        "reporter", "repository", "research team head", "research team member",
        "researcher", "respondent", "respondent-appellant", "respondent-appellee",
        "responsible party", "restager", "restorationist", "reviewer", "rubricator",
        "scenarist", "scientific advisor", "screenwriter", "scribe", "sculptor",
        "second party", "secretary", "seller", "set designer", "setting",
        "signer", "singer", "software developer", "sound designer", "sound engineer",
        "speaker", "special effects provider", "sponsor", "stage director",
        "stage manager", "standards body", "stereotyper", "storyteller",
        "supporting host", "surveyor", "teacher", "technical advisor",
        "technical director", "television director", "television guest",
        "television host", "television producer", "television writer",
        "thesis advisor", "transcriber", "translator", "type designer",
        "typographer", "university place", "videographer", "visual effects provider",
        "vocalist", "voice actor", "witness", "wood engraver", "woodcutter",
        "writer of accompanying material", "writer of added commentary",
        "writer of added lyrics", "writer of added text", "writer of afterword",
        "writer of film story", "writer of foreword", "writer of intertitles",
        "writer of introduction", "writer of preface", "writer of supplementary textual content",
        "writer of television story"
    ]:
        # Standard LOC relator - use directly
        return f"loc:{role}"
    else:
        # Unknown role - use as literal (fallback)
        return f"\"{role}\""

def _strip_ns(tag: str) -> str:
    """Return local part of a tag name (remove namespace)."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag

def _clean_uri(text: str) -> str:
    """Clean text for use in URIs."""
    if not text:
        return ""
    # Remove special characters and replace spaces with underscores
    cleaned = re.sub(r'[^\w\s-]', '', text)
    cleaned = re.sub(r'\s+', '_', cleaned.strip())
    return cleaned.lower()

def _generate_uri(base: str, identifier: str) -> str:
    """Generate a URI from base and identifier."""
    if not identifier:
        return base
    cleaned_id = _clean_uri(identifier)
    return f"{base.rstrip('/')}/{cleaned_id}"

def _escape_ttl_literal(text: str) -> str:
    """Escape text for use in TTL literals."""
    if not text:
        return ""
    
    # Replace problematic characters
    escaped = text.replace('\\', '\\\\')  # Backslash
    escaped = escaped.replace('"', '\\"')  # Double quote
    escaped = escaped.replace('\n', '\\n')  # Newline
    escaped = escaped.replace('\r', '\\r')  # Carriage return
    escaped = escaped.replace('\t', '\\t')  # Tab
    
    return escaped

def _extract_text_content(elem: ET.Element) -> str:
    """Extract text content from an element, handling mixed content."""
    text_parts = []
    if elem.text:
        text_parts.append(elem.text.strip())
    
    for child in elem:
        if child.text:
            text_parts.append(child.text.strip())
        if child.tail:
            text_parts.append(child.tail.strip())
    
    return " ".join(text_parts).strip()

def _parse_mei_metadata(path: Path | str):
    """Return a python dict representing the <meiHead> of *path*."""
    tree = ET.parse(path)
    root = tree.getroot()
    local_root = _strip_ns(root.tag)

    if local_root == "mei":
        mei_head = root.find("mei:meiHead", MEI_NS)
    elif local_root == "meiHead":
        mei_head = root
    else:
        raise ValueError("The provided file doesn't look like an MEI document.")

    if mei_head is None:
        raise ValueError("<meiHead> element not found – is the file valid MEI?")

    return mei_head

def generate_prov_ttl(mei_head: ET.Element, file_path: str) -> str:
    """Generate PROV-O RDF in Turtle format from MEI header."""
    
    # Base URI for the MEI file
    file_uri = f"mei:file/{_clean_uri(Path(file_path).name)}"
    
    # Generate URIs for different entities
    entities = {
        'file': file_uri,
        'work': f"mei:work/{_clean_uri(Path(file_path).stem)}",
        'edition': f"mei:edition/{_clean_uri(Path(file_path).stem)}_edition"
    }
    
    # Track agents and activities
    agents = {}
    activities = {}
    
    ttl_lines = []
    
    # Add namespace declarations
    ttl_lines.append("@prefix prov: <http://www.w3.org/ns/prov#> .")
    ttl_lines.append("@prefix foaf: <http://xmlns.com/foaf/0.1/> .")
    ttl_lines.append("@prefix dcterms: <http://purl.org/dc/terms/> .")
    ttl_lines.append("@prefix bibo: <http://purl.org/ontology/bibo/> .")
    ttl_lines.append("@prefix mei: <http://www.music-encoding.org/ns/mei#> .")
    ttl_lines.append("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .")
    ttl_lines.append("@prefix loc: <http://id.loc.gov/vocabulary/relators/> .")
    ttl_lines.append("@prefix skos: <http://www.w3.org/2004/02/skos/core#> .")
    ttl_lines.append("@prefix elaute: <https://e-laute.info/vocab#> .")
    ttl_lines.append("")
    
    # Define E-LAUTE specific roles as SKOS concepts
    ttl_lines.append("# E-LAUTE specific roles as SKOS concepts")
    ttl_lines.append("elaute:meiEditor a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:edt .")
    ttl_lines.append("elaute:fronimoEditor a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:edt .")
    ttl_lines.append("elaute:musescoreEditor a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:edt .")
    ttl_lines.append("elaute:metadataContact a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:ctb .")
    ttl_lines.append("elaute:intabulator a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:arr .")
    ttl_lines.append("elaute:provider a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:pbl .")
    ttl_lines.append("elaute:funder a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:fnd .")
    ttl_lines.append("elaute:publisher a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:pbl .")
    ttl_lines.append("elaute:author a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:aut .")
    ttl_lines.append("elaute:composer a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:cmp .")
    ttl_lines.append("elaute:scribe a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:scr .")
    ttl_lines.append("elaute:collector a skos:Concept ;")
    ttl_lines.append("    skos:broader loc:col .")
    ttl_lines.append("")
    
    # Define the MEI file entity
    ttl_lines.append(f"# MEI File Entity")
    ttl_lines.append(f"<{entities['file']}> a prov:Entity ;")
    ttl_lines.append(f"    dcterms:type \"MEI Music Encoding\" ;")
    ttl_lines.append(f"    dcterms:source \"{file_path}\" .")
    ttl_lines.append("")
    
    # Process fileDesc
    file_desc = mei_head.find("mei:fileDesc", MEI_NS)
    if file_desc is not None:
        ttl_lines.extend(_process_filedesc(file_desc, entities, agents, activities))
    
    # Process encodingDesc
    encoding_desc = mei_head.find("mei:encodingDesc", MEI_NS)
    if encoding_desc is not None:
        ttl_lines.extend(_process_encodingdesc(encoding_desc, entities, agents, activities))
    
    # Process revisionDesc
    revision_desc = mei_head.find("mei:revisionDesc", MEI_NS)
    if revision_desc is not None:
        ttl_lines.extend(_process_revisiondesc(revision_desc, entities, agents, activities))
    
    return "\n".join(ttl_lines)

def _process_filedesc(file_desc: ET.Element, entities: dict, agents: dict, activities: dict) -> list:
    """Process fileDesc element and generate PROV-O statements."""
    ttl_lines = []
    
    # Process titleStmt
    title_stmt = file_desc.find("mei:titleStmt", MEI_NS)
    if title_stmt is not None:
        ttl_lines.extend(_process_titlestmt(title_stmt, entities, agents))
    
    # Process editionStmt
    edition_stmt = file_desc.find("mei:editionStmt", MEI_NS)
    if edition_stmt is not None:
        ttl_lines.extend(_process_editionstmt(edition_stmt, entities, agents))
    
    # Process pubStmt
    pub_stmt = file_desc.find("mei:pubStmt", MEI_NS)
    if pub_stmt is not None:
        ttl_lines.extend(_process_pubstmt(pub_stmt, entities, agents))
    
    # Process sourceDesc
    source_desc = file_desc.find("mei:sourceDesc", MEI_NS)
    if source_desc is not None:
        ttl_lines.extend(_process_sourcedesc(source_desc, entities, agents))
    
    return ttl_lines

def _process_titlestmt(title_stmt: ET.Element, entities: dict, agents: dict) -> list:
    """Process titleStmt element."""
    ttl_lines = []
    
    # Process titles
    titles = title_stmt.findall("mei:title", MEI_NS)
    for i, title in enumerate(titles):
        title_type = title.get("type", "main")
        title_text = _extract_text_content(title)
        if title_text:
            escaped_title = _escape_ttl_literal(title_text)
            ttl_lines.append(f"<{entities['work']}> dcterms:title \"{escaped_title}\"@{title_type} .")
    
    # Process responsibility statements
    resp_stmts = title_stmt.findall("mei:respStmt", MEI_NS)
    for resp_stmt in resp_stmts:
        ttl_lines.extend(_process_respstmt(resp_stmt, entities, agents))
    
    return ttl_lines

def _process_respstmt(resp_stmt: ET.Element, entities: dict, agents: dict) -> list:
    """Process responsibility statement."""
    ttl_lines = []
    
    # Get the target entity (work, source, etc.)
    target_entity = entities.get('work', entities.get('source', None))
    if not target_entity:
        return ttl_lines
    
    # Process person names
    pers_names = resp_stmt.findall("mei:persName", MEI_NS)
    for pers_name in pers_names:
        role = pers_name.get("role", "contributor")
        agent_id = pers_name.get("{http://www.w3.org/XML/1998/namespace}id", "")
        
        if not agent_id:
            # Generate agent ID from name
            fore_name = pers_name.find("mei:foreName", MEI_NS)
            fam_name = pers_name.find("mei:famName", MEI_NS)
            if fore_name is not None and fam_name is not None:
                agent_id = f"agent_{_clean_uri(fore_name.text)}_{_clean_uri(fam_name.text)}"
        
        agent_uri = f"mei:agent/{agent_id}"
        agents[agent_id] = agent_uri
        
        # Define agent
        fore_name = pers_name.find("mei:foreName", MEI_NS)
        fam_name = pers_name.find("mei:famName", MEI_NS)
        
        if fore_name is not None and fam_name is not None:
            ttl_lines.append(f"<{agent_uri}> a foaf:Person ;")
            ttl_lines.append(f"    foaf:givenName \"{_escape_ttl_literal(fore_name.text)}\" ;")
            ttl_lines.append(f"    foaf:familyName \"{_escape_ttl_literal(fam_name.text)}\" .")
            
            # Link agent to target entity with role
            ttl_lines.append(f"<{target_entity}> dcterms:creator <{agent_uri}> .")
            role_uri = _get_role_uri(role)
            ttl_lines.append(f"<{agent_uri}> prov:hadRole {role_uri} .")
    
    # Process corporate names
    corp_names = resp_stmt.findall("mei:corpName", MEI_NS)
    for corp_name in corp_names:
        role = corp_name.get("role", "contributor")
        corp_id = corp_name.get("{http://www.w3.org/XML/1998/namespace}id", "")
        
        if not corp_id:
            # Generate ID from name
            corp_text = _extract_text_content(corp_name)
            corp_id = f"org_{_clean_uri(corp_text)}"
        
        corp_uri = f"mei:organization/{corp_id}"
        agents[corp_id] = corp_uri
        
        # Define organization
        corp_text = _extract_text_content(corp_name)
        if corp_text:
            ttl_lines.append(f"<{corp_uri}> a foaf:Organization ;")
            ttl_lines.append(f"    foaf:name \"{_escape_ttl_literal(corp_text)}\" .")
            
            # Link organization to target entity
            ttl_lines.append(f"<{target_entity}> dcterms:contributor <{corp_uri}> .")
            role_uri = _get_role_uri(role)
            ttl_lines.append(f"<{corp_uri}> prov:hadRole {role_uri} .")
    
    return ttl_lines

def _process_editionstmt(edition_stmt: ET.Element, entities: dict, agents: dict) -> list:
    """Process editionStmt element."""
    ttl_lines = []
    
    editions = edition_stmt.findall("mei:edition", MEI_NS)
    for edition in editions:
        edition_text = _extract_text_content(edition)
        edition_n = edition.get("n", "1")
        resp = edition.get("resp", "")
        
        if edition_text:
            ttl_lines.append(f"<{entities['edition']}> a bibo:Document ;")
            ttl_lines.append(f"    dcterms:description \"{_escape_ttl_literal(edition_text)}\" ;")
            ttl_lines.append(f"    dcterms:edition \"{edition_n}\" .")
            
            # Link edition to work
            ttl_lines.append(f"<{entities['work']}> bibo:edition <{entities['edition']}> .")
            
            # Link to responsible agent if available
            if resp and resp.startswith("#") and resp[1:] in agents:
                agent_uri = agents[resp[1:]]
                ttl_lines.append(f"<{entities['edition']}> dcterms:creator <{agent_uri}> .")
    
    return ttl_lines

def _process_pubstmt(pub_stmt: ET.Element, entities: dict, agents: dict) -> list:
    """Process pubStmt element."""
    ttl_lines = []
    
    # Process publisher
    publisher = pub_stmt.find("mei:publisher", MEI_NS)
    if publisher is not None:
        corp_name = publisher.find("mei:corpName", MEI_NS)
        if corp_name is not None:
            pub_text = _extract_text_content(corp_name)
            if pub_text:
                pub_id = f"publisher_{_clean_uri(pub_text)}"
                pub_uri = f"mei:organization/{pub_id}"
                agents[pub_id] = pub_uri
                
                ttl_lines.append(f"<{pub_uri}> a foaf:Organization ;")
                ttl_lines.append(f"    foaf:name \"{_escape_ttl_literal(pub_text)}\" .")
                ttl_lines.append(f"<{entities['work']}> dcterms:publisher <{pub_uri}> .")
    
    # Process date
    date_elem = pub_stmt.find("mei:date", MEI_NS)
    if date_elem is not None:
        date_text = _extract_text_content(date_elem)
        isodate = date_elem.get("isodate", "")
        if isodate:
            ttl_lines.append(f"<{entities['work']}> dcterms:issued \"{isodate}\"^^xsd:date .")
        elif date_text:
            ttl_lines.append(f"<{entities['work']}> dcterms:issued \"{_escape_ttl_literal(date_text)}\" .")
    
    # Process publication place
    pub_place = pub_stmt.find("mei:pubPlace", MEI_NS)
    if pub_place is not None:
        place_text = _extract_text_content(pub_place)
        if place_text:
            ttl_lines.append(f"<{entities['work']}> dcterms:spatial \"{_escape_ttl_literal(place_text)}\" .")
    
    # Process identifier
    identifier = pub_stmt.find("mei:identifier", MEI_NS)
    if identifier is not None:
        id_text = _extract_text_content(identifier)
        id_type = identifier.get("type", "PID")
        if id_text:
            ttl_lines.append(f"<{entities['work']}> dcterms:identifier \"{_escape_ttl_literal(id_text)}\" .")
            ttl_lines.append(f"<{entities['work']}> dcterms:identifierType \"{id_type}\" .")
    
    return ttl_lines

def _process_sourcedesc(source_desc: ET.Element, entities: dict, agents: dict) -> list:
    """Process sourceDesc element."""
    ttl_lines = []
    
    sources = source_desc.findall("mei:source", MEI_NS)
    for i, source in enumerate(sources):
        source_id = f"source_{i+1}"
        source_uri = f"mei:source/{source_id}"
        
        ttl_lines.append(f"<{source_uri}> a bibo:Document .")
        ttl_lines.append(f"<{entities['work']}> dcterms:source <{source_uri}> .")
        
        # Process bibliographic structure
        bibl_struct = source.find("mei:biblStruct", MEI_NS)
        if bibl_struct is not None:
            ttl_lines.extend(_process_biblstruct(bibl_struct, source_uri, agents))
    
    return ttl_lines

def _process_biblstruct(bibl_struct: ET.Element, source_uri: str, agents: dict) -> list:
    """Process bibliographic structure."""
    ttl_lines = []
    
    # Process analytic (article/contribution level)
    analytic = bibl_struct.find("mei:analytic", MEI_NS)
    if analytic is not None:
        title = analytic.find("mei:title", MEI_NS)
        if title is not None:
            title_text = _extract_text_content(title)
            if title_text:
                ttl_lines.append(f"<{source_uri}> dcterms:title \"{_escape_ttl_literal(title_text)}\" .")
        
        # Process analytic responsibility
        resp_stmt = analytic.find("mei:respStmt", MEI_NS)
        if resp_stmt is not None:
            ttl_lines.extend(_process_respstmt(resp_stmt, {"source": source_uri}, agents))
    
    # Process monogr (monograph level)
    monogr = bibl_struct.find("mei:monogr", MEI_NS)
    if monogr is not None:
        title = monogr.find("mei:title", MEI_NS)
        if title is not None:
            title_text = _extract_text_content(title)
            if title_text:
                ttl_lines.append(f"<{source_uri}> dcterms:isPartOf \"{_escape_ttl_literal(title_text)}\" .")
        
        # Process monogr responsibility
        resp_stmt = monogr.find("mei:respStmt", MEI_NS)
        if resp_stmt is not None:
            ttl_lines.extend(_process_respstmt(resp_stmt, {"source": source_uri}, agents))
        
        # Process imprint
        imprint = monogr.find("mei:imprint", MEI_NS)
        if imprint is not None:
            date = imprint.find("mei:date", MEI_NS)
            if date is not None:
                date_text = _extract_text_content(date)
                isodate = date.get("isodate", "")
                if isodate:
                    ttl_lines.append(f"<{source_uri}> dcterms:created \"{isodate}\"^^xsd:gYear .")
                elif date_text:
                    ttl_lines.append(f"<{source_uri}> dcterms:created \"{_escape_ttl_literal(date_text)}\" .")
    
    return ttl_lines

def _process_encodingdesc(encoding_desc: ET.Element, entities: dict, agents: dict, activities: dict) -> list:
    """Process encodingDesc element."""
    ttl_lines = []
    
    # Process appInfo
    app_info = encoding_desc.find("mei:appInfo", MEI_NS)
    if app_info is not None:
        ttl_lines.extend(_process_appinfo(app_info, entities, agents, activities))
    
    return ttl_lines

def _process_appinfo(app_info: ET.Element, entities: dict, agents: dict, activities: dict) -> list:
    """Process appInfo element."""
    ttl_lines = []
    
    applications = app_info.findall("mei:application", MEI_NS)
    for i, app in enumerate(applications):
        app_id = app.get("{http://www.w3.org/XML/1998/namespace}id", f"app_{i+1}")
        app_uri = f"mei:software/{app_id}"
        
        # Define software agent
        ttl_lines.append(f"<{app_uri}> a prov:SoftwareAgent .")
        
        # Get application name
        name_elem = app.find("mei:name", MEI_NS)
        if name_elem is not None:
            app_name = _extract_text_content(name_elem)
            if app_name:
                ttl_lines.append(f"<{app_uri}> foaf:name \"{_escape_ttl_literal(app_name)}\" .")
        
        # Get version
        version = app.get("version", "")
        if version:
            ttl_lines.append(f"<{app_uri}> dcterms:version \"{version}\" .")
        
        # Get dates
        start_date = app.get("startdate", "")
        end_date = app.get("enddate", "")
        if start_date:
            ttl_lines.append(f"<{app_uri}> prov:startedAtTime \"{start_date}\"^^xsd:dateTime .")
        if end_date:
            ttl_lines.append(f"<{app_uri}> prov:endedAtTime \"{end_date}\"^^xsd:dateTime .")
        
        # Create encoding activity
        activity_id = f"encoding_{app_id}"
        activity_uri = f"mei:activity/{activity_id}"
        activities[activity_id] = activity_uri
        
        ttl_lines.append(f"<{activity_uri}> a prov:Activity ;")
        ttl_lines.append(f"    prov:wasAssociatedWith <{app_uri}> ;")
        ttl_lines.append(f"    prov:used <{entities['work']}> ;")
        ttl_lines.append(f"    prov:generated <{entities['file']}> .")
        
        if start_date:
            ttl_lines.append(f"<{activity_uri}> prov:startedAtTime \"{start_date}\"^^xsd:dateTime .")
        if end_date:
            ttl_lines.append(f"<{activity_uri}> prov:endedAtTime \"{end_date}\"^^xsd:dateTime .")
    
    return ttl_lines

def _process_revisiondesc(revision_desc: ET.Element, entities: dict, agents: dict, activities: dict) -> list:
    """Process revisionDesc element."""
    ttl_lines = []
    
    changes = revision_desc.findall("mei:change", MEI_NS)
    for i, change in enumerate(changes):
        change_id = change.get("n", f"change_{i+1}")
        activity_uri = f"mei:activity/revision_{change_id}"
        activities[f"revision_{change_id}"] = activity_uri
        
        # Get change details
        change_desc = change.find("mei:changeDesc", MEI_NS)
        if change_desc is not None:
            p_elem = change_desc.find("mei:p", MEI_NS)
            if p_elem is not None:
                change_text = _extract_text_content(p_elem)
                if change_text:
                    ttl_lines.append(f"<{activity_uri}> a prov:Activity ;")
                    ttl_lines.append(f"    dcterms:description \"{_escape_ttl_literal(change_text)}\" .")
        
        # Get responsible agent
        resp = change.get("resp", "")
        if resp and resp.startswith("#") and resp[1:] in agents:
            agent_uri = agents[resp[1:]]
            ttl_lines.append(f"<{activity_uri}> prov:wasAssociatedWith <{agent_uri}> .")
        
        # Get date
        isodate = change.get("isodate", "")
        if isodate:
            ttl_lines.append(f"<{activity_uri}> prov:endedAtTime \"{isodate}\"^^xsd:date .")
        
        # Link to file
        ttl_lines.append(f"<{activity_uri}> prov:generated <{entities['file']}> .")
    
    return ttl_lines

def main(argv: list[str]):
    if len(argv) != 2 or argv[1] in {"-h", "--help"}:
        print(__doc__.strip())
        return 1

    mei_path = Path(argv[1])
    if not mei_path.is_file():
        print(f"Error: '{mei_path}' is not a file.", file=sys.stderr)
        return 2

    try:
        mei_head = _parse_mei_metadata(mei_path)
        ttl_output = generate_prov_ttl(mei_head, str(mei_path))
        print(ttl_output)
        return 0
    except Exception as e:
        print(f"Error processing file: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main(sys.argv)) 
