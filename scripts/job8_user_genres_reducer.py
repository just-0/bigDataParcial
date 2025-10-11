#!/usr/bin/env python3
"""
============================================================================
JOB 8: GÉNEROS POR USUARIO - REDUCER
============================================================================
Input: user_id \t genre:playcount (sorted by user_id)
Output: user_id \t top_genre1,top_genre2,top_genre3
============================================================================
"""
import sys
from collections import defaultdict

current_user = None
genre_plays = defaultdict(int)

def output_top_genres(user_id, genres_dict):
    """Output top 3 genres for user"""
    sorted_genres = sorted(genres_dict.items(), key=lambda x: x[1], reverse=True)
    top_3 = sorted_genres[:3]
    genres_str = ','.join([f"{g}:{p}" for g, p in top_3])
    print(f"{user_id}\t{genres_str}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        user_id, genre_play = line.split('\t')
        genre, playcount = genre_play.split(':')
        playcount = int(playcount)

        if current_user == user_id:
            genre_plays[genre] += playcount
        else:
            if current_user:
                output_top_genres(current_user, genre_plays)
            current_user = user_id
            genre_plays = defaultdict(int)
            genre_plays[genre] = playcount
    except Exception:
        continue

# Don't forget last user
if current_user:
    output_top_genres(current_user, genre_plays)
