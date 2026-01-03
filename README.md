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
1. Download Metadata
Initialize the project by downloading the base metadata file (vp_discours.json) from the French government open data portal.

```bash
bash download_metadata.sh
```

2. Crawl Speeches
The main crawler is located in src/collect_speeches.py. This script allows you to define a begin date and end date to filter which speeches to scrape. It iterates through the metadata, visits the source URLs, and extracts the full text content.
```bash
scrapy runspider ./src/collect_speeches.py
```

3. Analysis & Investigation
The notebooks/ directory contains tools to inspect the data:

- `data_investigate.ipynb`: Used to understand the structure of the source `vp_discours.json` file.
- `postprocessing.ipynb`: Used for cleaning and standardizing the output files.

## Output Data Schema
The crawler outputs a list of dictionary objects. Each dictionary represents a single speech event containing metadata and the full transcript.

Below is the definition of the keys contained in the output JSON:
| Key | Type | Description |
| :--- | :--- | :--- |
| **`id`** | String | The unique identifier for the speech document. |
| **`titre`** | String | The official title or headline of the speech. |
| **`url`** | String | The source URL where the full speech was scraped. |
| **`domaine`** | String | The broad political domain or office of the speaker (e.g., President of the Republic). |
| **`prononciation`** | Date (ISO) | The date the speech was actually delivered (`YYYY-MM-DD`). |
| **`intervenants`** | List[Dict] | A list of speakers involved. Contains nested keys for `nom` (name) and `qualite` (official title/role). |
| **`auteur_moral`** | List | Organizations or institutions credited as the author, if applicable. |
| **`circonstance`** | String | Contextual description of the event (e.g., "State visit to...", "Interview with..."). |
| **`type_emetteur`** | String | The category of the entity issuing the speech. |
| **`type_document`** | String | The format of the speech (e.g., Declaration, Interview, Press Conference). |
| **`type_media`** | String/Null | The media format if applicable (often null for text-only archives). |
| **`media`** | String/Null | Link to media files (audio/video) if available. |
| **`resume`** | String/Null | A short summary or abstract of the speech content. |
| **`thematiques`** | List[String]| Broad categories or themes associated with the speech (e.g., "International Relations"). |
| **`descripteurs`** | List[String]| Specific keywords or tags describing the content (e.g., specific countries, laws, or scientific topics). |
| **`mise_en_ligne`** | Date (ISO) | The date the record was published online. |
| **`mise_a_jour`** | Date (ISO) | The date the record was last updated. |
| **`source`** | String/Null | The original source attribution. |
| **`text`** | String | **The full scraped transcript of the speech.** This includes the body of the address, dialogue, or interview text. |