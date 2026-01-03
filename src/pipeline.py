import os
import re
import sys
import logging
import hashlib
import pandas as pd
import requests
import trafilatura
from lxml import etree
from datetime import datetime
from urllib.parse import urlparse
from tqdm import tqdm
from datasketch import MinHash, MinHashLSH
from easynmt import EasyNMT

# --- Configuration ---
class Config:
    # Official metadata from DILA (Direction de l'information légale et administrative)
    # Resource ID from data.gouv.fr (Vie-publique.fr metadata)
    METADATA_URL = "[https://www.data.gouv.fr/fr/datasets/r/884e9306-692a-4467-96a2-63229b4b0299](https://www.data.gouv.fr/fr/datasets/r/884e9306-692a-4467-96a2-63229b4b0299)" 
    
    # Local storage paths
    DATA_DIR = "data"
    RAW_DIR = os.path.join(DATA_DIR, "raw_text")
    HISTORICAL_DIR = os.path.join(DATA_DIR, "historical_xml") # Place ARTFL XML files here
    OUTPUT_FILE = os.path.join(DATA_DIR, "french_political_speeches_v1.parquet")
    
    # Deduplication settings
    MINHASH_PERMS = 128
    LSH_THRESHOLD = 0.90
    
    # Translation settings
    # Using NLLB-200-distilled for a balance of speed and quality
    MODEL_NAME = 'nllb-200-distilled-1.3B' 
    TARGET_LANG = 'en'
    BATCH_SIZE = 8

    # User agent for scraping to be polite
    USER_AGENT = "FrenchPoliticalSpeechCorpus/1.0 (Research Prototype; +[http://example.org](http://example.org))"

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    # handlers=
)
logger = logging.getLogger(__name__)

class CorpusBuilder:
    def __init__(self):
        self.metadata = pd.DataFrame()
        self.lsh = MinHashLSH(threshold=Config.LSH_THRESHOLD, num_perm=Config.MINHASH_PERMS)
        self.translator = None  # Lazy load
        
        # Ensure directories exist
        os.makedirs(Config.RAW_DIR, exist_ok=True)
        os.makedirs(Config.HISTORICAL_DIR, exist_ok=True)

    def download_metadata(self):
        """Step 1: Ingest official metadata from data.gouv.fr (Vie-publique)."""
        logger.info("Downloading metadata from data.gouv.fr...")
        try:
            # The DILA dataset usually contains: date, titre, intervenant, url_dossier, etc.
            self.metadata = pd.read_csv(Config.METADATA_URL, sep=';', encoding='utf-8', on_bad_lines='skip')
            
            # Normalize columns
            self.metadata.columns = [c.lower().strip() for c in self.metadata.columns]
            
            # Basic cleaning
            if 'date' in self.metadata.columns:
                self.metadata['date'] = pd.to_datetime(self.metadata['date'], errors='coerce')
                self.metadata['year'] = self.metadata['date'].dt.year
            
            # Filter for valid URLs
            self.metadata = self.metadata.dropna(subset=['url'])
            logger.info(f"Loaded {len(self.metadata)} records from Vie-publique metadata.")
            
        except Exception as e:
            logger.error(f"Failed to download metadata: {e}")
            sys.exit(1)

    def scrape_content(self, sample_size=None):
        """Step 2: Scrape text using Trafilatura."""
        logger.info("Starting acquisition pipeline...")
        
        # For prototype testing, allow limiting size
        df_to_process = self.metadata.head(sample_size) if sample_size else self.metadata
        
        results =
        
        for idx, row in tqdm(df_to_process.iterrows(), total=len(df_to_process), desc="Scraping"):
            url = row.get('url')
            doc_id = hashlib.md5(url.encode()).hexdigest()
            
            try:
                downloaded = trafilatura.fetch_url(url)
                if downloaded:
                    # Trafilatura heuristics for main content extraction
                    text = trafilatura.extract(
                        downloaded, 
                        include_comments=False, 
                        include_tables=False,
                        no_fallback=False
                    )
                    
                    if text:
                        results.append({
                            'id': doc_id,
                            'source': 'vie-publique',
                            'original_url': url,
                            'date': row.get('date'),
                            'year': row.get('year'),
                            'speaker': row.get('intervenant', 'Unknown'),
                            'title': row.get('titre', 'Unknown'),
                            'text_fr': text,
                            'text_en': None, # To be filled
                            'translation_type': 'none'
                        })
            except Exception as e:
                logger.warning(f"Failed to scrape {url}: {e}")
                
        self.corpus = pd.DataFrame(results)
        logger.info(f"Acquired {len(self.corpus)} documents.")

    def ingest_historical_xml(self):
        """Step 3: Parse TEI/XML files from Archives Parlementaires (ARTFL/Stanford)."""
        # Assumes XML files are downloaded locally in Config.HISTORICAL_DIR
        xml_files =
        
        if not xml_files:
            logger.warning("No historical XML files found. Skipping historical ingestion.")
            return

        logger.info(f"Parsing {len(xml_files)} historical XML files...")
        historical_records =

        for xml_file in xml_files:
            path = os.path.join(Config.HISTORICAL_DIR, xml_file)
            try:
                tree = etree.parse(path)
                root = tree.getroot()
                
                # Namespace handling for TEI
                ns = {'tei': '[http://www.tei-c.org/ns/1.0](http://www.tei-c.org/ns/1.0)'}
                
                # Simplified parsing logic for standard TEI speech tags <sp>
                speeches = root.findall('.//tei:sp', ns)
                
                for sp in speeches:
                    speaker = sp.xpath('.//tei:speaker/text()', namespaces=ns)
                    speaker_name = speaker if speaker else "Unknown"
                    
                    # Extract paragraphs
                    paragraphs = sp.xpath('.//tei:p/text()', namespaces=ns)
                    full_text = "\n".join(paragraphs).strip()
                    
                    if full_text:
                        historical_records.append({
                            'id': hashlib.md5(full_text[:500].encode()).hexdigest(),
                            'source': 'archives_parlementaires',
                            'original_url': 'local_xml',
                            'date': None, # Would parse from header in full impl
                            'year': 1789, # Placeholder/Example
                            'speaker': speaker_name,
                            'title': 'Debat Parlementaire',
                            'text_fr': full_text,
                            'text_en': None,
                            'translation_type': 'none'
                        })
            except Exception as e:
                logger.error(f"Error parsing XML {xml_file}: {e}")

        if historical_records:
            self.corpus = pd.concat(, ignore_index=True)
            logger.info(f"Added {len(historical_records)} historical records.")

    def deduplicate(self):
        """Step 4: Probabilistic Deduplication using MinHash LSH."""
        if self.corpus.empty:
            return

        logger.info("Running deduplication...")
        
        # Create MinHash for each document
        self.corpus['minhash'] = self.corpus['text_fr'].apply(
            lambda text: self._create_minhash(text)
        )
        
        # Index in LSH
        duplicates = set()
        for idx, row in self.corpus.iterrows():
            m = row['minhash']
            if m:
                # Query for similar items
                result = self.lsh.query(m)
                if result:
                    # Found duplicate(s)
                    duplicates.add(idx)
                    # Keep the one we just found, mark current as dupe (simple strategy)
                    # In production, we would compare metadata quality
                else:
                    self.lsh.insert(f"doc_{idx}", m)
        
        initial_len = len(self.corpus)
        # Drop duplicates (excluding the first occurrence logic handled by LSH insert order)
        # Note: A rigorous impl would handle the 'result' list carefully to keep the best version.
        # For prototype, we assume if query returns match, it's a dupe of something already seen.
        
        # Actually, simpler logic for prototype:
        keep_indices =
        seen_hashes = MinHashLSH(threshold=Config.LSH_THRESHOLD, num_perm=Config.MINHASH_PERMS)
        
        for idx, row in self.corpus.iterrows():
            m = row['minhash']
            if not m: continue
            
            if not seen_hashes.query(m):
                seen_hashes.insert(idx, m)
                keep_indices.append(idx)
        
        self.corpus = self.corpus.loc[keep_indices].drop(columns=['minhash'])
        logger.info(f"Deduplication finished. Removed {initial_len - len(self.corpus)} documents.")

    def _create_minhash(self, text):
        """Helper to create MinHash object from text."""
        if not isinstance(text, str) or len(text) < 50:
            return None
        m = MinHash(num_perm=Config.MINHASH_PERMS)
        # Simple shingling (3-grams)
        tokens = text.lower().split()
        for i in range(len(tokens) - 2):
            shingle = " ".join(tokens[i:i+3])
            m.update(shingle.encode('utf8'))
        return m

    def translate_corpus(self):
        """Step 5: Neural Machine Translation using EasyNMT (NLLB)."""
        if self.corpus.empty:
            return

        logger.info(f"Loading Translation Model ({Config.MODEL_NAME})...")
        # EasyNMT handles tokenization and document splitting automatically
        self.translator = EasyNMT(Config.MODEL_NAME)
        
        # Filter rows needing translation
        mask = self.corpus['text_en'].isna() & (self.corpus['text_fr'].notna())
        indices = self.corpus[mask].index
        
        logger.info(f"Translating {len(indices)} documents. This may take a while...")
        
        # Process in chunks to save progress
        chunk_size = 10  # Speeches are long, keep chunk small
        
        for i in tqdm(range(0, len(indices), chunk_size), desc="Translating"):
            batch_idx = indices[i : i + chunk_size]
            batch_texts = self.corpus.loc[batch_idx, 'text_fr'].tolist()
            
            try:
                # Translate batch
                translations = self.translator.translate(
                    batch_texts, 
                    source_lang='fr', 
                    target_lang=Config.TARGET_LANG,
                    batch_size=Config.BATCH_SIZE
                )
                
                # Update DataFrame
                self.corpus.loc[batch_idx, 'text_en'] = translations
                self.corpus.loc[batch_idx, 'translation_type'] = 'nmt_nllb_200'
                
            except Exception as e:
                logger.error(f"Translation batch failed: {e}")

    def export_dataset(self):
        """Step 6: Export to Parquet with documentation."""
        if self.corpus.empty:
            logger.warning("Corpus is empty. Nothing to export.")
            return

        logger.info(f"Exporting {len(self.corpus)} records to {Config.OUTPUT_FILE}...")
        
        # Enforce schema
        self.corpus['year'] = self.corpus['year'].fillna(0).astype(int)
        self.corpus['date'] = self.corpus['date'].astype(str)
        
        # Save using PyArrow engine for efficiency
        self.corpus.to_parquet(Config.OUTPUT_FILE, engine='pyarrow', index=False)
        
        # Generate Datasheet / README
        readme_path = os.path.join(Config.DATA_DIR, "README.md")
        with open(readme_path, "w") as f:
            f.write(f"# French Political Speech Corpus (Prototype)\n\n")
            f.write(f"**Generated:** {datetime.now().isoformat()}\n")
            f.write(f"**Total Documents:** {len(self.corpus)}\n")
            f.write(f"**Sources:** Vie-publique.fr, Archives Parlementaires\n")
            f.write(f"**Translation Model:** {Config.MODEL_NAME}\n")
            f.write(f"\n## Schema\n")
            f.write(f"- `id`: Unique hash\n- `text_fr`: Original French\n- `text_en`: English Translation\n")
        
        logger.info("Export complete.")

if __name__ == "__main__":
    builder = CorpusBuilder()
    
    # 1. Download official metadata index
    builder.download_metadata()
    
    # 2. Scrape content (limit to 5 for prototype speed)
    builder.scrape_content(sample_size=5)
    
    # 3. Ingest Historical Data (if files exist)
    builder.ingest_historical_xml()
    
    # 4. Deduplicate
    builder.deduplicate()
    
    # 5. Translate
    builder.translate_corpus()
    
    # 6. Export
    builder.export_dataset()