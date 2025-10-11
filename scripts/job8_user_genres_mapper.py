#!/usr/bin/env python3
"""
============================================================================
JOB 8: GÉNEROS POR USUARIO - MAPPER
============================================================================
Input: TSV con campos: user_id, genre, playcount
Output: user_id \t genre:playcount
============================================================================
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        fields = line.split('\t')
        if len(fields) >= 3:
            user_id = fields[0].strip()
            genre = fields[1].strip().lower()
            playcount = int(fields[2])

            if user_id and genre and genre != 'unknown':
                print(f"{user_id}\t{genre}:{playcount}")
    except Exception:
        continue
