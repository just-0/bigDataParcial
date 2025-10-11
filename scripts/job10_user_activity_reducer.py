#!/usr/bin/env python3
"""
============================================================================
JOB 10: ACTIVIDAD DE USUARIOS - REDUCER
============================================================================
Input: user_id \t songs_count,plays (sorted by user_id)
Output: user_id \t total_songs,total_plays,user_type
============================================================================
"""
import sys

current_user = None
total_songs = 0
total_plays = 0

def classify_user(songs, plays):
    """Classify user by activity level"""
    if songs < 5:
        return "casual"
    elif songs < 20:
        return "regular"
    elif songs < 50:
        return "active"
    elif songs < 100:
        return "heavy"
    else:
        return "power_user"

def output_user(user_id, songs, plays):
    """Output user statistics"""
    user_type = classify_user(songs, plays)
    print(f"{user_id}\t{songs}\t{plays}\t{user_type}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        user_id, values = line.split('\t')
        song_count, playcount = values.split(',')

        song_count = int(song_count)
        playcount = int(playcount)

        if current_user == user_id:
            total_songs += song_count
            total_plays += playcount
        else:
            if current_user:
                output_user(current_user, total_songs, total_plays)
            current_user = user_id
            total_songs = song_count
            total_plays = playcount
    except Exception:
        continue

# Don't forget last user
if current_user:
    output_user(current_user, total_songs, total_plays)
