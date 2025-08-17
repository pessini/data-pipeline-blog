import streamlit as st
import pandas as pd
import altair as alt
import json
from modules import s3, database
from modules.utils import get_target_games, format_game_list
from modules.config import CACHE_TTL_SECONDS

# Page configuration
st.set_page_config(
    page_title="Brazilian Lottery Dashboard",
    page_icon="🎲",
    layout="wide"
)

# Main title
st.title("🎲 Brazilian Lottery Dashboard")
st.divider()

# Initialize data service with caching
@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_cached_db_path():
    """Download and cache the DuckDB file"""
    return s3.download_duckdb_file()

# Test S3 connection
is_connected, status_msg = s3.test_s3_connection()
if not is_connected:
    st.error(f"❌ {status_msg}")

# Get cached database path
db_path = get_cached_db_path()

# Get available games dynamically
available_games = database.get_available_games(db_path) if db_path else []

col1, col2, col3 = st.columns([1, 1, 1])
# Game filter
if available_games:
    lottery_type = col1.selectbox(
        "Select Lottery Game",
        ["All Games"] + available_games,
        key="game_selector"
    )
else:
    st.warning("No lottery games found in database")
    lottery_type = "All Games"

st.divider()
selected_game = None if lottery_type == "All Games" else lottery_type

# Fetch and display lottery data
if is_connected and db_path:
    # Fetch latest draws for the selected game
    lottery_data = database.get_latest_draws(db_path, limit=5, game_name=selected_game) if selected_game else database.get_latest_draws(db_path, limit=5, distinct_games=True)

    if lottery_data is not None and not lottery_data.empty:
        # Process the data for better display
        display_data = lottery_data.copy()

        # Format winning numbers (assuming JSON format)
        if 'winning_numbers' in display_data.columns:
            display_data['Winning Numbers'] = display_data['winning_numbers'].apply(
                lambda x: ', '.join(map(str, json.loads(x) if isinstance(x, str) else x)) if x else 'N/A'
            )

        # Format draw date
        if 'draw_date' in display_data.columns:
            display_data['Draw Date'] = pd.to_datetime(display_data['draw_date']).dt.strftime('%Y-%m-%d')

        # Select columns to display
        cols_to_display = ['game_name', 'draw_number', 'Draw Date', 'Winning Numbers']
        display_cols = [col for col in cols_to_display if col in display_data.columns]

        # Rename columns for better display
        column_mapping = {
            'game_name': 'Game',
            'draw_number': 'Draw #',
        }

        final_display = display_data[display_cols].rename(columns=column_mapping)

        st.subheader(f"🎯 Last 5 draws")
        # Display as table
        st.dataframe(
            final_display,
            use_container_width=True,
            hide_index=True
        )

    else:
        st.warning("No lottery data found.")

    # Show number frequency chart only for specific games (not "All Games")
    if selected_game is not None:
        st.divider()

        # Create columns for title and frequency filter
        col1, col2 = st.columns([3, 1])

        with col1:
            st.subheader(f"📊 Most Frequent Numbers - {selected_game}")

        with col2:
            frequency_limit = st.number_input(
                "Top Numbers",
                min_value=5,
                max_value=50,
                value=10,
                step=1,
                key=f"frequency_limit_{selected_game}"
            )

        # Get number frequency data
        frequency_data = database.get_number_frequency(db_path, selected_game, limit=frequency_limit)

        if frequency_data is not None and not frequency_data.empty:
            # Create Altair bar chart with modern syntax
            click_selection = alt.selection_point()

            # Calculate dynamic height based on number of items
            dynamic_height = max(300, frequency_limit * 30 + 100)

            chart = alt.Chart(frequency_data).mark_bar(
                opacity=0.8
            ).add_params(
                click_selection
            ).encode(
                x=alt.X('frequency:Q', 
                       title='Frequency'),
                y=alt.Y('number:O', 
                       title='Number',
                       sort='-x'),
                tooltip=[
                    alt.Tooltip('number:O', title='Number'),
                    alt.Tooltip('frequency:Q', title='Times Drawn')
                ],
                color=alt.Color(
                    'frequency:Q',
                    scale=alt.Scale(
                        range=['lightsteelblue', 'steelblue', 'darkblue']
                    ),
                    legend=None
                )
            ).properties(
                width=600,
                height=dynamic_height,
                title=f'Top {frequency_limit} Most Frequent Numbers in {selected_game}'
            ).configure_title(
                fontSize=16,
                fontWeight='bold',
                anchor='start'
            ).configure_axis(
                labelFontSize=12,
                titleFontSize=14,
                grid=False
            )

            # Display the chart
            st.altair_chart(chart, use_container_width=True)

            # Add least frequent numbers chart
            st.markdown("---")

            # Create columns for title and frequency filter for least frequent
            col3, col4 = st.columns([3, 1])

            with col3:
                st.subheader(f"📉 Least Frequent Numbers - {selected_game}")

            with col4:
                least_frequency_limit = st.number_input(
                    "Bottom Numbers",
                    min_value=5,
                    max_value=50,
                    value=10,
                    step=1,
                    key=f"least_frequency_limit_{selected_game}"
                )

            # Get least frequent number data
            least_frequency_data = database.get_least_frequent_numbers(db_path, selected_game, limit=least_frequency_limit)

            if least_frequency_data is not None and not least_frequency_data.empty:
                # Create Altair bar chart for least frequent numbers
                least_click_selection = alt.selection_point()

                # Calculate dynamic height based on number of items
                least_dynamic_height = max(300, least_frequency_limit * 30 + 100)

                least_chart = alt.Chart(least_frequency_data).mark_bar(
                    opacity=0.8
                ).add_params(
                    least_click_selection
                ).encode(
                    x=alt.X('frequency:Q', 
                           title='Frequency'),
                    y=alt.Y('number:O', 
                           title='Number',
                           sort='x'),  # Sort ascending for least frequent
                    tooltip=[
                        alt.Tooltip('number:O', title='Number'),
                        alt.Tooltip('frequency:Q', title='Times Drawn')
                    ],
                    color=alt.Color(
                        'frequency:Q',
                        scale=alt.Scale(
                            range=['lightcoral', 'orangered', 'darkred']
                        ),
                        legend=None
                    )
                ).properties(
                    width=600,
                    height=least_dynamic_height,
                    title=f'Bottom {least_frequency_limit} Least Frequent Numbers in {selected_game}'
                ).configure_title(
                    fontSize=16,
                    fontWeight='bold',
                    anchor='start'
                ).configure_axis(
                    labelFontSize=12,
                    titleFontSize=14,
                    grid=False
                )

                # Display the least frequent chart
                st.altair_chart(least_chart, use_container_width=True)
            else:
                st.warning(f"No least frequent data available for {selected_game}")
        else:
            st.warning(f"No frequency data available for {selected_game}")

    # Fetch full data for selected game
    with st.expander(f"Full data {" - "+selected_game if selected_game else ""}"):
        full_data = database.get_latest_draws(db_path, limit=None, game_name=selected_game)
        if full_data is not None and not full_data.empty:
            st.dataframe(full_data, use_container_width=True, hide_index=True)
else:
    st.error("Cannot connect to the database. Please check your configuration.")
