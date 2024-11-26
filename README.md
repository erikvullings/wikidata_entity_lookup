# WikiData entity service

For OSINT and PII (Personal Identifiable Information) analysis, it is practical to have an offline reference to WikiData.

- For PII, names of public persons and organizations can be made exempt from removal. Therefore, the names of public persons and organizations are extracted to a CSV. This file can also be used to enhance named entities to their Entity ID.
- For OSINT, those names, and the names of other entities like locations and events, are extracted to key-value pairs for easy lookup based on entity ID. 
 
Thus the pipeline would be as follows: An extracted article would be processed using NER (Named Entity Recognition). Each relevant person, location, etc. would be checked if it is present in the extracted Wikidata data. If so, the article is not only enhanced with the NER, but also with the Wikidata ID. The OSINT analyst can, when reading the article, use the Wikidata ID to retrieve the Wikidata data to enhance his information and understanding.

Therefore, this repository:
1. Extracts entities of interest from Wikidata and saves them to JSON Lines or MessagePack format.
2. Stores the names and aliases in a CSV: This file can be used for PII, or to enhance extracted named entities with their entity ID.
3. Loads the extracted entities into a KeyDB key-value store for easy lookup. This KeyDB service is shared as a Docker image including data for offline usage.

## Prerequisites

Download the latest WikiData entry, e.g. you can use aria2 to download it more efficiently (in 2024, it is 130Gb):

```ps1
# In case you need to install it
choco install aria2
aria2c.exe -x 16 https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz
```

Unzip the data to `latest-all.json` using 7zip or some efficient unzipper that shows progress, as 1.5Tb takes some time to unzip.

## Run

```bash
cargo run --release -- /d/data/wikidata/latest-all.json -e person,organisation -o ./output -f JSONLines
```

## Host the data online

> WiP 
> 
> The generated output, either in JSON lines or MessagePack format, must be loaded into KeyDB. See the Docker folder. 