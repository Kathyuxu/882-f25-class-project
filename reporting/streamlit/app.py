# app.py

import os
import duckdb
import pandas as pd
import streamlit as st

from google.cloud import secretmanager

project_id = 'btibert-ba882-fall25'
secret_id = 'MotherDuck'   #<---------- this is the name of the secret you created
version_id = 'latest'

sm = secretmanager.SecretManagerServiceClient()

name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")

md = duckdb.connect(f'md:?motherduck_token={md_token}') 

# ------------------------------------------------------------------------------
# Page setup
# ------------------------------------------------------------------------------
st.set_page_config(page_title="My Fancy Streamlit App", layout="wide")
st.title("BA882 — Streamlit Basics")
st.caption("Demo app reading from **MotherDuck** → schema: `nfl.stage`")

SCHEMA = "nfl.stage"



# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def fetch_df(query: str, params: tuple | None = None) -> pd.DataFrame:
    if params:
        return md.execute(query, params).df()
    return md.sql(query).df()

# ------------------------------------------------------------------------------
# Sidebar filters (assumes tables exist)
# ------------------------------------------------------------------------------
with st.sidebar:
    st.header("Filters")

    # Seasons
    seasons_df = fetch_df(
        f"SELECT DISTINCT season FROM {SCHEMA}.dim_games ORDER BY season DESC"
    )
    seasons = seasons_df["season"].tolist()
    season = st.selectbox("Season", seasons, index=0 if seasons else None)

    # Weeks within the selected season
    weeks_df = fetch_df(
        f"""
        SELECT DISTINCT "week" AS week
        FROM {SCHEMA}.dim_games
        WHERE season = ?
        ORDER BY week
        """,
        (int(season),),
    ) if season is not None else pd.DataFrame(columns=["week"])
    weeks = weeks_df["week"].tolist()
    week = st.selectbox("Week", weeks, index=0 if weeks else None)

    st.markdown("These filters do not actually work, but highlight you can provide user input widgets to allow stakeholders to view your data!")

# ------------------------------------------------------------------------------
# KPI metrics
# ------------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)
with col1:
    n_teams = fetch_df(f"SELECT COUNT(*) AS n FROM {SCHEMA}.dim_teams")["n"].iat[0]
    st.metric("Teams", n_teams)

with col2:
    n_games = fetch_df(f"SELECT COUNT(*) AS n FROM {SCHEMA}.dim_games")["n"].iat[0]
    st.metric("Games", n_games)

with col3:
    n_articles = fetch_df(f"SELECT COUNT(*) AS n FROM {SCHEMA}.dim_articles")["n"].iat[0]
    st.metric("Articles", n_articles)

with col4:
    n_players = fetch_df(
        f"SELECT COUNT(DISTINCT athlete_id) AS n FROM {SCHEMA}.fact_player_stats"
    )["n"].iat[0]
    st.metric("Players (stats)", n_players)

st.divider()

# ------------------------------------------------------------------------------
# Tabs
# ------------------------------------------------------------------------------
tab_overview, tab_games, tab_articles, tab_players = st.tabs(
    ["Overview", "Games", "Articles", "Players"]
)

# ---------------------------------- Overview -----------------------------------------
with tab_overview:
    st.subheader("Games per Week (selected season)")
    if season is not None:
        gpw = fetch_df(
            f"""
            SELECT "week" AS week, COUNT(*) AS games
            FROM {SCHEMA}.dim_games
            WHERE season = ?
            GROUP BY week
            ORDER BY week
            """,
            (int(season),),
        )
        if gpw.empty:
            st.info("No games for this season.")
        else:
            st.bar_chart(gpw.set_index("week")["games"])
            st.dataframe(gpw, use_container_width=True)
    else:
        st.info("Select a season to view the weekly distribution.")

# ----------------------------------- Games -------------------------------------------
with tab_games:
    st.subheader("Game List & Scores")
    if season is not None and week is not None:
        games = fetch_df(
            f"""
            WITH base AS (
                SELECT
                    g.id AS game_id,
                    g.season,
                    g."week" AS week,
                    CAST(f.team_id AS INTEGER) AS team_id,
                    f.home_away,
                    f.score
                FROM {SCHEMA}.dim_games g
                JOIN {SCHEMA}.fact_game_team f ON f.game_id = g.id
                WHERE g.season = ? AND g."week" = ?
            )
            SELECT
                b.game_id,
                b.season,
                b.week,
                t.abbrev AS team,
                b.home_away,
                b.score
            FROM base b
            LEFT JOIN {SCHEMA}.dim_teams t ON t.id = b.team_id
            ORDER BY game_id, home_away DESC
            """,
            (int(season), int(week)),
        )
        if games.empty:
            st.info("No games found for the selected week.")
        else:
            st.dataframe(games, use_container_width=True)

            # Wide score view: one row per game with HOME/AWAY scores
            pivot = (
                games.pivot_table(
                    index=["game_id", "season", "week"],
                    columns="home_away",
                    values="score",
                    aggfunc="max",
                )
                .reset_index()
                .rename_axis(None, axis=1)
            )
            st.markdown("**Scores by Game (wide view)**")
            st.dataframe(pivot, use_container_width=True)
    else:
        st.info("Select a season & week to see games and scores.")

# ---------------------------------- Articles -----------------------------------------
with tab_articles:
    st.subheader("Articles & Images (by game)")
    # game picker constrained by selected season/week when available
    if season is not None and week is not None:
        game_opts = fetch_df(
            f"""
            SELECT id AS game_id
            FROM {SCHEMA}.dim_games
            WHERE season = ? AND "week" = ?
            ORDER BY id
            """,
            (int(season), int(week)),
        )
    elif season is not None:
        game_opts = fetch_df(
            f"""
            SELECT id AS game_id
            FROM {SCHEMA}.dim_games
            WHERE season = ?
            ORDER BY id
            """,
            (int(season),),
        )
    else:
        game_opts = fetch_df(f"SELECT id AS game_id FROM {SCHEMA}.dim_games ORDER BY id")

    if game_opts.empty:
        st.info("No games available to select.")
    else:
        game_id = st.selectbox("Game", game_opts["game_id"].tolist())
        articles = fetch_df(
            f"""
            SELECT id, headline, published, source, story
            FROM {SCHEMA}.dim_articles
            WHERE game_id = ?
            ORDER BY published DESC NULLS LAST
            LIMIT 20
            """,
            (int(game_id),),
        )
        if articles.empty:
            st.info("No articles for this game.")
        else:
            st.dataframe(articles, use_container_width=True)

        # Images (if present for the selected game)
        imgs = fetch_df(
            f"""
            SELECT url, caption, height, width
            FROM {SCHEMA}.dim_article_images
            WHERE game_id = ?
            LIMIT 6
            """,
            (int(game_id),),
        )
        if not imgs.empty:
            st.markdown("**Images**")
            cols = st.columns(3)
            for i, row in imgs.iterrows():
                with cols[i % 3]:
                    st.image(
                        row["url"],
                        caption=row.get("caption", None),
                        use_container_width=True,
                    )

# ---------------------------------- Players ------------------------------------------
with tab_players:
    st.subheader("Player Stats (raw explorer)")
    # Optional: filter by game
    filter_by_game = st.checkbox("Filter by game", value=False)
    params = ()
    where_clause = ""
    if filter_by_game:
        game_ids = fetch_df(f"SELECT id AS game_id FROM {SCHEMA}.dim_games ORDER BY id")
        if not game_ids.empty:
            chosen = st.selectbox("Game", game_ids["game_id"].tolist())
            where_clause = "WHERE fps.game_id = ?"
            params = (int(chosen),)

    df_players = fetch_df(
        f"""
        SELECT
            fps.game_id,
            fps.athlete_id,
            fps.athlete_name,
            fps.category,
            fps.stat_key,
            fps.stat_label,
            fps.value_str,
            fps.team_id
        FROM {SCHEMA}.fact_player_stats fps
        {where_clause}
        ORDER BY fps.game_id, fps.athlete_name
        LIMIT 200
        """,
        params,
    )
    if df_players.empty:
        st.info("No player stats to display.")
    else:
        st.dataframe(df_players, use_container_width=True)

# ------------------------------------------------------------------------------
# Teaching notes (toggle)
# ------------------------------------------------------------------------------
with st.expander("What this app demonstrates (teaching notes)"):
    st.markdown(
        """
- **Layout & UI**: `set_page_config`, columns, tabs, sidebar filters, expanders.
- **Query pattern**: `fetch_df(sql, params)` → Pandas → `st.*` to render.
- **Caching**: `@st.cache_data` so repeated interactions don’t re-query.
- **Metrics**: Quick KPIs via `st.metric`.
- **Charts**: `st.bar_chart` with a tidy DataFrame.
- **Tables**: `st.dataframe` for interactive sorting/resizing.
- **Images**: `st.image` from article URLs in `dim_article_images`.
- **SQL style**: Explicit schema prefix (`nfl.stage`) and quoting `"week"`.
        """
    )
