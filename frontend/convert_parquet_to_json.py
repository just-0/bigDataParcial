#!/usr/bin/env python3
"""
Convierte archivos Parquet de S3 a JSON para el frontend
"""
import pandas as pd
import json
import os
from pathlib import Path

def convert_parquet_folder(folder_path, output_json):
    """Convierte todos los parquet de una carpeta a un JSON"""
    parquet_files = list(Path(folder_path).glob("*.parquet")) + list(Path(folder_path).glob("**/000000_0*"))

    if not parquet_files:
        print(f"No parquet files found in {folder_path}")
        return

    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        combined.to_json(output_json, orient='records', indent=2)
        print(f"[OK] {output_json} ({len(combined)} records)")

def convert_mapreduce_output(folder_path, output_json):
    """Convierte output de MapReduce (TSV) a JSON"""
    part_files = list(Path(folder_path).glob("part-*"))

    if not part_files:
        print(f"No output files in {folder_path}")
        return

    data = []
    for file in part_files:
        with open(file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if line:
                    data.append(line.split('\t'))

    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"[OK] {output_json} ({len(data)} records)")

# Create output directory
os.makedirs('data/json', exist_ok=True)

print("Converting Job 6 Sample (Charts)...")
convert_parquet_folder('data/charts/top_20_sample', 'data/json/top_20_sample.json')

print("\nConverting Job 7 (Artist Plays)...")
convert_mapreduce_output('data/job7_artist_plays', 'data/json/artist_plays.json')

print("\nConverting Job 8 (User Genres)...")
convert_mapreduce_output('data/job8_user_genres', 'data/json/user_genres.json')

print("\nConverting Job 9 (Decade Stats)...")
convert_mapreduce_output('data/job9_decade_stats', 'data/json/decade_stats.json')

print("\nConverting Job 10 (User Activity)...")
convert_mapreduce_output('data/job10_user_activity', 'data/json/user_activity.json')

print("\n[SUCCESS] Conversion complete!")
