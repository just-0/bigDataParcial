#!/usr/bin/env python3
"""
============================================================================
JOB 9: ESTADÍSTICAS POR DÉCADA - MAPPER
============================================================================
Input: TSV: year, danceability, energy, tempo, valence
Output: decade \t song_count:1,sum_dance:X,sum_energy:Y,sum_tempo:Z,sum_valence:W
============================================================================
"""
import sys

def get_decade(year):
    """Convert year to decade"""
    try:
        y = int(year)
        if y == 0:
            return "Unknown"
        elif y < 1950:
            return "Pre-1950"
        elif y < 1960:
            return "1950s"
        elif y < 1970:
            return "1960s"
        elif y < 1980:
            return "1970s"
        elif y < 1990:
            return "1980s"
        elif y < 2000:
            return "1990s"
        elif y < 2010:
            return "2000s"
        else:
            return "2010s+"
    except:
        return "Unknown"

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        fields = line.split('\t')
        if len(fields) >= 5:
            year = fields[0]
            danceability = float(fields[1]) if fields[1] else 0
            energy = float(fields[2]) if fields[2] else 0
            tempo = float(fields[3]) if fields[3] else 0
            valence = float(fields[4]) if fields[4] else 0

            decade = get_decade(year)
            print(f"{decade}\t1,{danceability},{energy},{tempo},{valence}")
    except Exception:
        continue
