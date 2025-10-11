#!/usr/bin/env python3
"""
============================================================================
JOB 7: CONTEO DE PLAYS POR ARTISTA - REDUCER
============================================================================
Input: artist \t playcount (sorted by artist)
Output: artist \t total_plays
============================================================================
"""
import sys

current_artist = None
total_plays = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        artist, playcount = line.split('\t')
        playcount = int(playcount)

        if current_artist == artist:
            total_plays += playcount
        else:
            if current_artist:
                print(f"{current_artist}\t{total_plays}")
            current_artist = artist
            total_plays = playcount
    except Exception:
        continue

# Don't forget last artist
if current_artist:
    print(f"{current_artist}\t{total_plays}")
