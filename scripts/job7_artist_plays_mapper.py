#!/usr/bin/env python3
"""
============================================================================
JOB 7: CONTEO DE PLAYS POR ARTISTA - MAPPER
============================================================================
Input: TSV con campos: user_id, track_id, playcount, artist
Output: artist \t playcount
============================================================================
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        fields = line.split('\t')
        if len(fields) >= 4:
            artist = fields[3].lower().strip()
            playcount = int(fields[2])

            if artist and artist != 'artist':
                print(f"{artist}\t{playcount}")
    except Exception:
        continue
