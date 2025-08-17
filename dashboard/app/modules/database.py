import duckdb
import streamlit as st
import pandas as pd
import json
from .utils import (
    get_target_games, 
    get_database_games, 
    display_name_to_db_name, 
    db_name_to_display_name,
    is_target_game, 
    validate_limit
)


def connect_to_database(db_path):
    """Connect to the DuckDB database
    
    Args:
        db_path: Path to the DuckDB file
        
    Returns:
        DuckDB connection object or None if failed
    """
    if not db_path:
        return None
    
    try:
        return duckdb.connect(db_path)
    except Exception as e:
        st.error(f"Failed to connect to DuckDB: {str(e)}")
        return None


def get_latest_draws(db_path, limit=5, game_name=None, distinct_games=False):
    """Get the latest lottery draws from DuckDB (filtered to target games)
    
    Args:
        db_path: Path to the DuckDB file
        limit: Number of draws to return (default: None for all draws)
        game_name: Display name of the game (e.g., 'Lotofácil')
        distinct_games: If True, return only the latest draw for each game
    """
    # Validate the limit if provided
    limit = validate_limit(limit) if limit else None

    conn = connect_to_database(db_path)
    if not conn:
        return None

    try:
        # Get database game names for filtering
        database_games = get_database_games()
        db_games_filter = "'" + "', '".join(database_games) + "'"

        base_query = f"""
            SELECT 
                game_name,
                draw_number,
                draw_date,
                winning_numbers,
                prize_tiers
            FROM lottery_results
            WHERE game_name IN ({db_games_filter})
        """

        if game_name and is_target_game(game_name, is_display_name=True):
            db_game_name = display_name_to_db_name(game_name)
            base_query += f" AND game_name = '{db_game_name}'"

        if distinct_games:
            base_query += " QUALIFY ROW_NUMBER() OVER (PARTITION BY game_name ORDER BY draw_date DESC, draw_number DESC) = 1"
        elif limit:
            base_query += f" ORDER BY draw_date DESC, draw_number DESC LIMIT {limit}"
        else:
            base_query += " ORDER BY draw_date DESC, draw_number DESC"

        result = conn.execute(base_query).fetchdf()
        conn.close()

        # Convert database names back to display names in the result
        if not result.empty and 'game_name' in result.columns:
            result['game_name'] = result['game_name'].apply(db_name_to_display_name)

        return result

    except Exception as e:
        st.error(f"Failed to query DuckDB: {str(e)}")
        if conn:
            conn.close()
        return None


def get_available_games(db_path):
    """Get list of available lottery games (returns display names)
    
    Args:
        db_path: Path to the DuckDB file
    """
    conn = connect_to_database(db_path)
    if not conn:
        return []
    
    try:
        # Get database game names for filtering
        database_games = get_database_games()
        target_games = get_target_games()
        
        # Filter for only our target games using database names
        db_games_filter = "'" + "', '".join(database_games) + "'"
        query = f"SELECT DISTINCT game_name FROM lottery_results WHERE game_name IN ({db_games_filter}) ORDER BY game_name"
        
        result = conn.execute(query).fetchall()
        conn.close()
        
        # Convert database names to display names
        available_db_games = [row[0] for row in result]
        available_display_games = [db_name_to_display_name(db_game) for db_game in available_db_games]
        
        # Return games in the order they appear in our target list (display names)
        ordered_games = []
        for target_game in target_games:
            if target_game in available_display_games:
                ordered_games.append(target_game)
        
        return ordered_games
        
    except Exception as e:
        st.error(f"Failed to get available games: {str(e)}")
        if conn:
            conn.close()
        return []


def get_winning_numbers(db_path, game_name, limit=10):
    """Get winning numbers for a specific game
    
    Args:
        db_path: Path to the DuckDB file
        game_name: Display name of the game (e.g., 'Lotofácil')
        limit: Number of results to return
    """
    limit = validate_limit(limit)
    
    if not is_target_game(game_name, is_display_name=True):
        st.error(f"Game '{game_name}' is not in target games list")
        return None
    
    conn = connect_to_database(db_path)
    if not conn:
        return None
    
    try:
        # Convert display name to database name for query
        db_game_name = display_name_to_db_name(game_name)
        
        query = """
            SELECT 
                draw_number,
                draw_date,
                winning_numbers
            FROM lottery_results
            WHERE game_name = ?
            ORDER BY draw_date DESC, draw_number DESC
            LIMIT ?
        """
        
        result = conn.execute(query, [db_game_name, limit]).fetchdf()
        conn.close()
        
        return result
        
    except Exception as e:
        st.error(f"Failed to get winning numbers: {str(e)}")
        if conn:
            conn.close()
        return None


def get_prize_tiers(db_path, game_name, limit=10):
    """Get prize tier information for a specific game
    
    Args:
        db_path: Path to the DuckDB file
        game_name: Display name of the game (e.g., 'Lotofácil')
        limit: Number of results to return
    """
    limit = validate_limit(limit)
    
    if not is_target_game(game_name, is_display_name=True):
        st.error(f"Game '{game_name}' is not in target games list")
        return None
    
    conn = connect_to_database(db_path)
    if not conn:
        return None
    
    try:
        # Convert display name to database name for query
        db_game_name = display_name_to_db_name(game_name)
        
        query = """
            SELECT 
                draw_number,
                draw_date,
                prize_tiers
            FROM lottery_results
            WHERE game_name = ?
            ORDER BY draw_date DESC, draw_number DESC
            LIMIT ?
        """
        
        result = conn.execute(query, [db_game_name, limit]).fetchdf()
        conn.close()
        
        return result
        
    except Exception as e:
        st.error(f"Failed to get prize tiers: {str(e)}")
        if conn:
            conn.close()
        return None


def get_draw_by_number(db_path, game_name, draw_number):
    """Get a specific draw by its number
    
    Args:
        db_path: Path to the DuckDB file
        game_name: Display name of the game (e.g., 'Lotofácil')
        draw_number: The draw number to retrieve
    """
    if not is_target_game(game_name, is_display_name=True):
        st.error(f"Game '{game_name}' is not in target games list")
        return None
    
    conn = connect_to_database(db_path)
    if not conn:
        return None
    
    try:
        # Convert display name to database name for query
        db_game_name = display_name_to_db_name(game_name)
        
        query = """
            SELECT 
                game_name,
                draw_number,
                draw_date,
                winning_numbers,
                prize_tiers
            FROM lottery_results
            WHERE game_name = ? AND draw_number = ?
        """
        
        result = conn.execute(query, [db_game_name, draw_number]).fetchdf()
        conn.close()
        
        # Convert database name back to display name in the result
        if not result.empty and 'game_name' in result.columns:
            result['game_name'] = result['game_name'].apply(db_name_to_display_name)
        
        return result
        
    except Exception as e:
        st.error(f"Failed to get draw by number: {str(e)}")
        if conn:
            conn.close()
        return None


def get_number_frequency(db_path, game_name, limit=10, ascending=False):
    """Get the frequency of each number for a specific game
    
    Args:
        db_path: Path to the DuckDB file
        game_name: Display name of the game (e.g., 'Lotofácil')
        limit: Number of numbers to return (default: 10)
        ascending: If True, returns least frequent numbers; if False, returns most frequent (default: False)
    
    Returns:
        DataFrame with columns: number, frequency
    """
    if not is_target_game(game_name, is_display_name=True):
        st.error(f"Game '{game_name}' is not in target games list")
        return None
    
    conn = connect_to_database(db_path)
    if not conn:
        return None
    
    try:
        # Convert display name to database name for query
        db_game_name = display_name_to_db_name(game_name)
        
        # Determine sort order based on ascending parameter
        sort_order = "ASC" if ascending else "DESC"
        
        # Since winning_numbers is JSON array of strings like ["00", "06", "11", ...]
        # Use DuckDB's JSON array unnesting with UNNEST function
        query = f"""
        WITH unnested_numbers AS (
            SELECT 
                CASE WHEN number_str = '00' THEN 0 ELSE CAST(TRIM(LEADING '0' FROM number_str) AS INTEGER) END AS number
            FROM lottery_results,
                UNNEST(json_extract_string(winning_numbers, '$[*]')) AS t(number_str)
            WHERE game_name = ?
            AND winning_numbers IS NOT NULL
            AND number_str IS NOT NULL
            AND TRIM(number_str) != ''
            AND TRIM(number_str) ~ '^[0-9]+$'
        )
        SELECT 
            number,
            COUNT(*) AS frequency
        FROM unnested_numbers
        GROUP BY number
        ORDER BY frequency {sort_order}, number ASC
        LIMIT ?
        """
        
        result = conn.execute(query, [db_game_name, limit]).fetchdf()
        conn.close()
        
        return result
        
    except Exception as e:
        frequency_type = "least frequent" if ascending else "most frequent"
        st.error(f"Failed to get {frequency_type} numbers: {str(e)}")
        if conn:
            conn.close()
        return None


def get_least_frequent_numbers(db_path, game_name, limit=10):
    """Get the least frequent numbers for a specific game
    
    Args:
        db_path: Path to the DuckDB file
        game_name: Display name of the game (e.g., 'Lotofácil')
        limit: Number of bottom numbers to return (default: 10)
    
    Returns:
        DataFrame with columns: number, frequency
    """
    return get_number_frequency(db_path, game_name, limit, ascending=True)
