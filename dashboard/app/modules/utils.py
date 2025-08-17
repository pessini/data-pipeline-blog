"""
Utility functions for the Brazilian Lottery Dashboard
"""

from .config import (
    TARGET_LOTTERY_GAMES, 
    DATABASE_LOTTERY_GAMES, 
    LOTTERY_GAMES_MAPPING,
    CACHE_TTL_SECONDS, 
    DEFAULT_LIMIT, 
    MAX_LIMIT
)

def get_target_games():
    """Get the list of target lottery games (display names)"""
    return TARGET_LOTTERY_GAMES.copy()

def get_database_games():
    """Get the list of database lottery game names"""
    return DATABASE_LOTTERY_GAMES.copy()

def get_games_mapping():
    """Get the complete mapping dictionary"""
    return LOTTERY_GAMES_MAPPING.copy()

def display_name_to_db_name(display_name):
    """Convert display name to database name"""
    return LOTTERY_GAMES_MAPPING.get(display_name, display_name)

def db_name_to_display_name(db_name):
    """Convert database name to display name"""
    # Create reverse mapping
    reverse_mapping = {v: k for k, v in LOTTERY_GAMES_MAPPING.items()}
    return reverse_mapping.get(db_name, db_name)

def is_target_game(game_name, is_display_name=True):
    """Check if a game is in our target list
    
    Args:
        game_name: The game name to check
        is_display_name: If True, treat game_name as display name. If False, treat as database name.
    """
    if is_display_name:
        return game_name in TARGET_LOTTERY_GAMES
    else:
        return game_name in DATABASE_LOTTERY_GAMES

def get_cache_ttl():
    """Get the cache TTL in seconds"""
    return CACHE_TTL_SECONDS

def get_default_limit():
    """Get the default limit for data queries"""
    return DEFAULT_LIMIT

def get_max_limit():
    """Get the maximum limit for data queries"""
    return MAX_LIMIT

def format_game_list(separator=', ', use_display_names=True):
    """Format the target games list as a string
    
    Args:
        separator: String to join game names
        use_display_names: If True, use display names. If False, use database names.
    """
    games = TARGET_LOTTERY_GAMES if use_display_names else DATABASE_LOTTERY_GAMES
    return separator.join(games)

def validate_limit(limit):
    """Validate and clamp limit to allowed range"""
    if limit is None:
        return DEFAULT_LIMIT
    return max(1, min(limit, MAX_LIMIT))
