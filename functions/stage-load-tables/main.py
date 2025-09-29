# this function will seed the stage schema tables in Motherduck

import functions_framework
from google.cloud import secretmanager
import duckdb

# settings
project_id = 'btibert-ba882-fall25'
secret_id = 'MotherDuck'   #<---------- this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'nfl'
schema = "stage"
db_schema = f"{db}.{schema}"

@functions_framework.http
def task(request):

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version in order to get the MotherDuck token
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    # this syntax lets us connect to our motherduck cloud warehouse and execute commands via the duckdb library

    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    ##################################################### create the core tables in the raw schema

    # dim teams --- pulls the latest records from stage
    raw_tbl_sql = """
    WITH latest AS (
    SELECT
        id, name, abbrev, display_name, short_name,
        color, alternate_color, venue_id, logo,
        ingest_timestamp, source_path, run_id
    FROM (
        SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM nfl.raw.teams AS t
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO nfl.stage.dim_teams
    SELECT
    id, name, abbrev, display_name, short_name,
    color, alternate_color, venue_id, logo,
    ingest_timestamp, source_path, run_id
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # dim venues --- pulls the latest records from stage
    raw_tbl_sql = """
    WITH latest AS (
    SELECT
        id, fullname, city, state, country, indoor,
        ingest_timestamp, source_path, run_id
    FROM (
        SELECT
        v.*,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM nfl.raw.venues AS v
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO nfl.stage.dim_venues
    SELECT *
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # dim games --- pulls the latest records from stage
    raw_tbl_sql = """
    WITH latest AS (
    SELECT
        id, start_date, season, "week", venue_id, attendance,
        ingest_timestamp, source_path, run_id
    FROM (
        SELECT
        g.*,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM nfl.raw.games AS g
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO nfl.stage.dim_games
    SELECT *
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # fact game team --- pulls the latest records from stage
    raw_tbl_sql = """
    WITH latest AS (
    SELECT
        game_id, team_id, home_away, score,
        ingest_timestamp, source_path, run_id
    FROM (
        SELECT
        g.*,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, team_id
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM nfl.raw.game_team AS g
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO nfl.stage.fact_game_team
    SELECT
    game_id, team_id, home_away, score,
    ingest_timestamp, source_path, run_id
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # dim articles--- pulls the latest records from stage
    raw_tbl_sql = """
    WITH latest AS (
    SELECT
        id, headline, published, source, story, game_id,
        ingest_timestamp, source_path, run_id
    FROM (
        SELECT
        a.*,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY ingest_timestamp DESC NULLS LAST
        ) AS rn
        FROM nfl.raw.articles AS a
    ) AS ranked
    WHERE rn = 1
    )
    INSERT OR REPLACE INTO nfl.stage.dim_articles
    SELECT *
    FROM latest;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # dim article images --- pulls the latest records from stage
    # qualify is not only duckdb logic, a little more direct
    raw_tbl_sql = """
    INSERT OR REPLACE INTO nfl.stage.dim_article_images (
    article_id, url, caption, height, width,
    game_id, ingest_timestamp, source_path, run_id
    )
    SELECT
    article_id, url, caption, height, width,
    game_id, ingest_timestamp, source_path, run_id
    FROM nfl.raw.article_images
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY article_id, url
    ORDER BY ingest_timestamp DESC NULLS LAST
    ) = 1;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # fact team stats
    raw_tbl_sql = """
    INSERT OR REPLACE INTO nfl.stage.fact_team_stats (
    category, displayValue, value, label,
    team_id, team_abbr, home_away,
    game_id, ingest_timestamp, source_path, run_id
    )
    SELECT
    "name"         AS category,
    displayValue,
    "value"        AS value,
    "label"        AS label,
    team_id,
    team_abbr,
    home_away,
    game_id,
    ingest_timestamp,
    source_path,
    run_id
    FROM nfl.raw.team_stats
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY game_id, team_id, "name"
    ORDER BY ingest_timestamp DESC NULLS LAST
    ) = 1;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)

    # player stats
    raw_tbl_sql = """
    INSERT OR REPLACE INTO nfl.stage.fact_player_stats (
    game_id, athlete_id, stat_key,
    team_id, athlete_name, category, stat_label, value_str,
    ingest_timestamp, source_path, run_id
    )
    SELECT
    game_id,
    athlete_id,
    stat_key,
    team_id,
    athlete_name,
    category,
    stat_label,
    value_str,
    ingest_timestamp,
    source_path,
    run_id
    FROM nfl.raw.player_stats
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY game_id, athlete_id, stat_key
    ORDER BY ingest_timestamp DESC NULLS LAST
    ) = 1;
    """
    print(f"{raw_tbl_sql}")
    md.execute(raw_tbl_sql)


    # return a dictionary/json entry, its blank because are not returning data, 200 for success
    return {}, 200