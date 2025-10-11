#!/usr/bin/env python3
"""
============================================================================
JOB 9: ESTADÍSTICAS POR DÉCADA - REDUCER
============================================================================
Input: decade \t count,danceability,energy,tempo,valence (sorted by decade)
Output: decade \t count,avg_dance,avg_energy,avg_tempo,avg_valence
============================================================================
"""
import sys

current_decade = None
total_count = 0
sum_dance = 0.0
sum_energy = 0.0
sum_tempo = 0.0
sum_valence = 0.0

def output_stats(decade, count, dance, energy, tempo, valence):
    """Output average statistics"""
    if count > 0:
        avg_dance = round(dance / count, 3)
        avg_energy = round(energy / count, 3)
        avg_tempo = round(tempo / count, 2)
        avg_valence = round(valence / count, 3)
        print(f"{decade}\t{count}\t{avg_dance}\t{avg_energy}\t{avg_tempo}\t{avg_valence}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        decade, values = line.split('\t')
        count, dance, energy, tempo, valence = values.split(',')

        count = int(count)
        dance = float(dance)
        energy = float(energy)
        tempo = float(tempo)
        valence = float(valence)

        if current_decade == decade:
            total_count += count
            sum_dance += dance
            sum_energy += energy
            sum_tempo += tempo
            sum_valence += valence
        else:
            if current_decade:
                output_stats(current_decade, total_count, sum_dance, sum_energy, sum_tempo, sum_valence)
            current_decade = decade
            total_count = count
            sum_dance = dance
            sum_energy = energy
            sum_tempo = tempo
            sum_valence = valence
    except Exception:
        continue

# Don't forget last decade
if current_decade:
    output_stats(current_decade, total_count, sum_dance, sum_energy, sum_tempo, sum_valence)
