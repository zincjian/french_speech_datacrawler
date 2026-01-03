import pandas as pd
import re

def clean_french_text(text):
    """Removes noise specific to French political speeches."""
    # 1. Remove parenthetical annotations like (Applaudissements) or (Rires)
    text = re.sub(r'\(.*?\)', '', text)
    # 2. Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def normalize_pipeline(input_path):
    df = pd.read_csv(input_path)
    
    # DEDUPLICATION: Remove identical speeches based on content hash
    df['text_hash'] = df['text'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
    df = df.drop_duplicates(subset=['text_hash'])
    
    # DATA NORMALIZATION
    df['text_clean'] = df['text'].apply(clean_french_text)
    
    # DATE CONVERSION: Convert French dates (e.g., '12 mai 2005') to ISO
    # You may need a library like 'dateparser' for French locales
    # df['date_iso'] = pd.to_datetime(df['date']) 

    df.to_parquet("data/france_speeches_final.parquet") # Better for large text data
    print("Dataset curated and exported to Parquet.")

normalize_pipeline("data/raw_speeches_2000_2010.csv")