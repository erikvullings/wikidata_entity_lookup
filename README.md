# WikiData entity service

For OSINT and PII (Personal Identifiable Information) analysis, it is practical to have an offline reference to WikiData.

- For PII, names of public persons and organizations can be made exempt from removal. Therefore, the names of public persons and organizations are extracted to a CSV. This file can also be used to enhance named entities to their Entity ID.
- For OSINT, those names, and the names of other entities like locations and events, are extracted to key-value pairs for easy lookup based on entity ID. 
 
Thus the pipeline would be as follows: An extracted article would be processed using NER (Named Entity Recognition). Each relevant person, location, etc. would be checked if it is present in the extracted Wikidata data. If so, the article is not only enhanced with the NER, but also with the Wikidata ID. The OSINT analyst can, when reading the article, use the Wikidata ID to retrieve the Wikidata data to enhance his information and understanding.

Therefore, this repository:

1. Extracts entities of interest from Wikidata and saves them to JSON Lines or MessagePack format.
2. Stores the names, short names and nicknames in a CSV: This file can be used for PII, or to enhance extracted named entities with their entity ID.
3. Extract additional properties for the main entities that are stores: uses online service to resolve them (saved in `output/entity_cache.csv`);
4. Loads the extracted entities into a KeyDB key-value store for easy lookup. This KeyDB service is shared as a Docker image including data for offline usage.

## Prerequisites

Download the latest WikiData entry, e.g. you can use aria2 to download it more efficiently (in 2024, it is 130Gb):

```ps1
# In case you need to install it
choco install aria2
aria2c.exe -x 16 https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz
```

Unzip the data to `latest-all.json` using 7zip or some efficient unzipper that shows progress, as 1.5Tb takes some time to unzip.

## Run

Using English as the main language:

```bash
cargo run --release -- /d/data/wikidata/latest-all.json -o ./output
```

Alternatively, specify the language, e.g. using Dutch:

```bash
cargo run --release -- /d/data/wikidata/latest-all.json -l nl -o ./output
```

Alternatively, on Windows:

```ps1
cargo run --release D:\data\wikidata\latest-all.json -l nl -o output
```

## Host the data online

> WiP 
> 
> The generated output, either in JSON lines or MessagePack format, must be loaded into KeyDB. See the Docker folder. 
>

## Queries

[Script source](https://github.com/kermitt2/grisp/blob/master/scripts/wikipedia-resources.sh).

To get the language-specific labels of the Wikibase properties `Pxxx`, download the properties in JSON format using a SPARQL query. For example, replace `<TWO_LETTER_LANGUAGE_CODE>` with `en` for English or `nl` for Dutch:

```bash
 wget "https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fproperty%20%3FpropertyLabel%20WHERE%20%7B%0A%20%20%20%20%3Fproperty%20a%20wikibase%3AProperty%20.%0A%20%20%20%20SERVICE%20wikibase%3Alabel%20%7B%0A%20%20%20%20%20%20bd%3AserviceParam%20wikibase%3Alanguage%20%22<TWO_LETTER_LANGUAGE_CODE>%22%20.%0A%20%20%20%7D%0A%20%7D%0A%0A" -O wikidata-<TWO_LETTER_LANGUAGE_CODE>-properties.json
 ```

To get the latest page properties, language links and actual articles:

```PS1
 aria2c.exe -x 16 https://dumps.wikimedia.org/<TWO_LETTER_LANGUAGE_CODE>wiki/latest/<TWO_LETTER_LANGUAGE_CODE>wiki-latest-pages-articles-multistream.xml.bz2
 aria2c.exe -x 16 https://dumps.wikimedia.org/<TWO_LETTER_LANGUAGE_CODE>wiki/latest/<TWO_LETTER_LANGUAGE_CODE>wiki-latest-page_props.sql.gz
 aria2c.exe -x 16 https://dumps.wikimedia.org/<TWO_LETTER_LANGUAGE_CODE>wiki/latest/<TWO_LETTER_LANGUAGE_CODE>wiki-latest-langlinks.sql.gz
 ```

- Latest pages contains the Wikimedia text represented as XML, and can be converted into an HTML page. The crate [`parse_wiki_text`](https://crates.io/crates/parse_wiki_text) parses the Wikimedia text to an AST (representation as Rust objects).
- Latest page properties has an index (`pp_page`) that refers to the index of the page block, e.g. Albert Speer has index 1, and is also the first page in the latest pages xml. In addition, it contains the Wikibase identifier, e.g. [Q60045](https://www.wikidata.org/wiki/Q60045), which refers to Albert Speer too, but now using a Q identifier.
- Loop through the language array to create translation files for each target language `(<PAGE_ID>, <TO_LANGUAGE_CODE>, <NAMED_ENTITY_IN_TO_LANGUAGE>)`, e.g. if the NER entity in a German text is Andre Agassi, it refers to `(2,'de','Andre Agassi')` or `(2,'awa','आन्द्रे अगासी')` in Awadhi, so entry 2 in the page properties and pages articles represents the tennis player `Andre Agassi`. In this case, the names are the same, but this is not always the case. For example, `(378,'en','Der Blaue Reiter')` in English becomes `(378,'az','Göy atlı')` in Azerbaijan.

So if you want to perform NER on a German text, and you want to display the results in Dutch, you take the `nlwiki-latest-langlinks.sql.gz` and extract all triplets that link the `de` language code to a page id and translation. Next, you compare a NER entity from the German text to the translated version. If there is a match, you can lookup the Wikibase identifier from the page props, and get the information text from pages articles.