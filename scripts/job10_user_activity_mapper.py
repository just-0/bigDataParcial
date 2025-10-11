#!/usr/bin/env python3
"""
============================================================================
JOB 10: ACTIVIDAD DE USUARIOS - MAPPER
============================================================================
Input: TSV: user_id, track_id, playcount
Output: user_id \t songs:1,plays:X
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
            playcount = int(fields[2])

            if user_id:
                print(f"{user_id}\t1,{playcount}")
    except Exception:
        continue
