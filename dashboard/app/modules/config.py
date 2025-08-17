"""
Configuration file for the Brazilian Lottery Dashboard
Contains global settings and constants used throughout the application
"""

# Target lottery games mapping: display_name -> database_name
LOTTERY_GAMES_MAPPING = {
    'Lotofácil': 'lotofacil',
    'Lotomania': 'lotomania', 
    'Quina': 'quina',
    'Mega-Sena': 'megasena',
    'Dia de Sorte': 'diadesorte'
}

# Get display names (keys) for interface
TARGET_LOTTERY_GAMES = list(LOTTERY_GAMES_MAPPING.keys())

# Get database names (values) for queries
DATABASE_LOTTERY_GAMES = list(LOTTERY_GAMES_MAPPING.values())

# Cache settings
CACHE_TTL_SECONDS = 3600  # 1 hour

# Display settings
DEFAULT_LIMIT = 5
MAX_LIMIT = 50
