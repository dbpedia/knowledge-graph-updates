"""
Description: Publish DNB KG files on the DBpedia Databus.
Author: Milan Dojchinovski
Email: dojcinovski.milan@gmail.com
Date: 2025-06-03
License: CC BY 4.0
"""

import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from collections import defaultdict

BASE_URL = "https://data.dnb.de/opendata/"
CHECKSUM_FILE = urljoin(BASE_URL, "001_Pruefsumme_Checksum.txt")
DATABUS_API = "https://databus.dbpedia.org/api/publish"
API_KEY = "YOUR_DATABUS_API_KEY"  # Replace with your real API key

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY,
    "Content-Type": "application/ld+json"
}

def fetch_checksums():
    response = requests.get(CHECKSUM_FILE)
    response.raise_for_status()
    checksums = {}
    for line in response.text.strip().splitlines():
        parts = line.strip().split(maxsplit=1)
        if len(parts) == 2:
            checksum, filename = parts
            checksums[filename] = checksum
    return checksums

def extract_description(link_tag):
    """Extracts the text after the link tag as description."""
    texts = []
    for sibling in link_tag.next_siblings:
        if sibling.name == 'img' or (hasattr(sibling, 'name') and sibling.name == 'a'):
            break
        if isinstance(sibling, str):
            texts.append(sibling.strip())
        elif sibling.name is None:
            texts.append(str(sibling).strip())
    return " ".join(filter(None, texts)).strip()

def fetch_gnd_data():
    response = requests.get(BASE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    checksums = fetch_checksums()

    grouped_files = defaultdict(list)
    # Match base names with multiple underscores before the date
    pattern = re.compile(r'^(authorities-gnd(?:[_\-][^_]+)*)_([0-9]{8})\.(.+?)\.gz$')

    for link in soup.find_all('a', href=True):
        href = link['href']
        if not href.startswith("authorities-gnd") or not re.search(r'\d{8}', href):
            continue

        match = pattern.match(href)
        if not match:
            continue

        base_name, date, ext = match.groups()
        filename = href
        full_url = urljoin(BASE_URL, filename)
        checksum = checksums.get(filename, "N/A")
        description = extract_description(link)

        #print(full_url)
        #print(checksum)
        #print(description)

        grouped_files[(base_name, date)].append({
            "url": full_url,
            "format": ext,
            "compression": "gz",
            "checksum": checksum,
            "description": description
        })


    return grouped_files

def publish_to_databus(grouped_files):
    for (base_name, date), files in grouped_files.items():
        artifact_name = base_name
        version_id = f"https://databus.dbpedia.org/m1ci/dnb/{artifact_name}/{date}"
        print(version_id)
        distributions = []

        # Use description from the first file in the group
        description = files[0].get("description", "data dump")

        for entry in files:
            distributions.append({
                "@type": "Part",
                "formatExtension": entry["format"],
                "compression": entry["compression"],
                "downloadURL": entry["url"]
            })

        payload = {
            "@context": "https://databus.dbpedia.org/res/context.jsonld",
            "@graph": [{
                "@type": "Version",
                "@id": version_id,
                "title": artifact_name,
                "description": description,
                "license": "https://dalicc.net/licenselibrary/Cc010Universal",
                "distribution": distributions
            }]
        }

        response = requests.post(DATABUS_API, headers=HEADERS, json=payload)
        if response.ok:
            print(f"✅ Published {artifact_name} ({date}) successfully.")
        else:
            print(f"❌ Failed to publish {artifact_name} ({date}): {response.status_code} - {response.text}")

if __name__ == "__main__":
    data = fetch_gnd_data()
    publish_to_databus(data)
