# french_speech_datacrawler
This repository contains tools to download, crawl, and process French political speeches from the vie-publique.fr public dataset. It allows for the collection of speech metadata and the scraping of full transcripts based on specific time durations.

## Repository Structure
```
french_speech_datacrawler/
├── download_metadata.sh       # Script to download the initial dataset from the French gov
├── src/
│   └── collect_speeches.py    # Main crawler script to fetch speech text
├── notebooks/                 # Jupyter notebooks for analysis
│   ├── data_investigate.ipynb # Initial exploration of vp_discours.json
│   ├── postprocessing.ipynb   # Data cleaning and formatting
│   └── testing_datacrawler.ipynb
├── dataset/                   # Directory for raw and processed data
└── logs/                      # Crawler logs and failure reports
```

## Usage
0. Environment Setup
Ensuring the needed packages are installed.
```bash
pip install -r requirements.txt
```
1. Download Metadata
Initialize the project by downloading the base metadata file (vp_discours.json) from the French government open data portal.

```bash
bash download_metadata.sh
```

2. Crawl Speeches
The main crawler is located in src/collect_speeches.py. This script allows you to define a begin date and end date to filter which speeches to scrape. It iterates through the metadata, visits the source URLs, and extracts the full text content. Output is an update dictionary with `text` as speech scripts and `source` specified in speeches text.
- **[IMPORTANT] Default crawling duration**: 2000-01-01 to 2010-12-31
- The valid duration is between 1959-01-15 to 2025-12-22, feel free to change `BEGIN_DATE_STR`
`END_DATE_STR` in `collect_speeches.py`
```bash
scrapy runspider ./src/collect_speeches.py
```

3. Analysis & Investigation
The notebooks/ directory contains tools to inspect the data:

- `data_investigate.ipynb`: Used to understand the structure of the source `vp_discours.json` file.
- `postprocessing.ipynb`: Used for cleaning and standardizing the output files.
    - Pipeline Steps:
        - Key Translation: Renames all JSON keys from French to English for better accessibility (e.g., intervenants $\to$ speakers).
        - Nested Standardization: Translates nested keys within the speakers list (e.g., qualite $\to$ speaker_role).
        - Data Cleaning: 
            - Drop Empty Columns: Automatically removes columns with >95% missing data (e.g., resume/summary is dropped).
            - Drop Empty Rows: Removes speech records where the scraped text body is null, empty, or whitespace.
        - Deduplication: specific logic to remove duplicates based on the unique id field.
        - Export: Saves the final clean dataset as a JSON file (e.g., _clean.json).
## Output Data Schema
- Dataset Example: [Speeches from 2000 to 2010](https://drive.google.com/file/d/1d0HAm2WS6145lluBPPdEwd6og6mBzKJ6/view?usp=sharing)

The crawler outputs a list of dictionary objects. Each dictionary represents a single speech event containing metadata and the full transcript.

Below is the definition of the keys contained in the output JSON:
| Original Key (French) | English Key | Type | Description |
| :--- | :--- | :--- | :--- |
| **`id`** | ID | String | The unique identifier for the speech document. |
| **`titre`** | Title | String | The official title or headline of the speech. |
| **`url`** | URL | String | The source URL where the full speech was scraped. |
| **`domaine`** | Domain | String | The broad political domain or office of the speaker (e.g., President of the Republic). |
| **`prononciation`** | Date Delivered | Date (ISO) | The date the speech was actually delivered (`YYYY-MM-DD`). |
| **`intervenants`** | Speakers | List[Dict] | A list of speakers involved. Contains nested keys for `nom` (name) and `qualite` (official title/role). |
| **`auteur_moral`** | Institutional Author | List | Organizations or institutions credited as the author, if applicable. |
| **`circonstance`** | Circumstance | String | Contextual description of the event (e.g., "State visit to...", "Interview with..."). |
| **`type_emetteur`** | Issuer Type | String | The category of the entity issuing the speech. |
| **`type_document`** | Document Type | String | The format of the speech (e.g., Declaration, Interview, Press Conference). |
| **`type_media`** | Media Type | String/Null | The media format if applicable (often null for text-only archives). |
| **`media`** | Media Link | String/Null | Link to media files (audio/video) if available. |
| **`resume`** | Summary | String/Null | A short summary or abstract of the speech content. (Removed in the final output, because most of this rows are empty) |
| **`thematiques`** | Themes | List[String]| Broad categories or themes associated with the speech (e.g., "International Relations"). |
| **`descripteurs`** | Descriptors | List[String]| Specific keywords or tags describing the content (e.g., specific countries, laws, or topics). |
| **`mise_en_ligne`** | Online Date | Date (ISO) | The date the record was published online. |
| **`mise_a_jour`** | Update Date | Date (ISO) | The date the record was last updated. |
| **`source`** | Source | String/Null | The original source attribution. |
| **`texte`** | Text | String | **The full scraped transcript of the speech.** This includes the body of the address, dialogue, or interview text. |
> Note: The `resume` (Summary) field is present in the raw data but is typically dropped during post-processing due to high missing values (>95%).