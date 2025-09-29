# this function will setup the stage schema in Motherduck

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

    ##################################################### create the database and schema

    # create the schema
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};") 

    ##################################################### create the core tables in the raw schema

    # teams
    raw_tbl_name = f"{db_schema}.dim_teams"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        abbrev VARCHAR,
        display_name VARCHAR,
        short_name VARCHAR,
        color VARCHAR,
        alternate_color VARCHAR,
        venue_id INTEGER,
        logo VARCHAR,
        ingest_timestamp TIMESTAMP,
        source_path VARCHAR,
        run_id VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # venues
    raw_tbl_name = f"{db_schema}.dim_venues"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        id               INTEGER PRIMARY KEY,
        fullname         VARCHAR,
        city             VARCHAR,
        state            VARCHAR,
        country          VARCHAR,
        indoor           BOOLEAN,
        ingest_timestamp TIMESTAMP,
        source_path      VARCHAR,
        run_id           VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # games
    raw_tbl_name = f"{db_schema}.dim_games"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        id               INTEGER PRIMARY KEY,
        start_date       TIMESTAMP,
        season           INTEGER,
        "week"           INTEGER,
        venue_id         INTEGER,
        attendance       INTEGER,
        ingest_timestamp TIMESTAMP,
        source_path      VARCHAR,
        run_id           VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # game team
    raw_tbl_name = f"{db_schema}.fact_game_team"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        game_id   INTEGER NOT NULL,
        team_id   INTEGER NOT NULL,
        home_away VARCHAR,
        score     INTEGER,
        ingest_timestamp TIMESTAMP,
        source_path      VARCHAR,
        run_id           VARCHAR,
        PRIMARY KEY (game_id, team_id)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # articles
    raw_tbl_name = f"{db_schema}.dim_articles"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        id               INTEGER PRIMARY KEY,
        headline         VARCHAR,
        published        TIMESTAMP,
        source           VARCHAR,
        story            VARCHAR,
        game_id          INTEGER,
        ingest_timestamp TIMESTAMP,
        source_path      VARCHAR,
        run_id           VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)


    # article images
    raw_tbl_name = f"{db_schema}.dim_article_images"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        article_id       INTEGER NOT NULL,
        url              VARCHAR NOT NULL,
        caption          VARCHAR,
        height           INTEGER,
        width            INTEGER,
        game_id          INTEGER,
        ingest_timestamp TIMESTAMP,
        source_path      VARCHAR,
        run_id           VARCHAR,
        PRIMARY KEY (article_id, url)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # article images
    raw_tbl_name = f"{db_schema}.dim_article_images"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        article_id       INTEGER NOT NULL,
        url              VARCHAR NOT NULL,
        caption          VARCHAR,
        height           INTEGER,
        width            INTEGER,
        game_id          INTEGER,
        ingest_timestamp TIMESTAMP,
        source_path      VARCHAR,
        run_id           VARCHAR,
        PRIMARY KEY (article_id, url)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # team stats
    raw_tbl_name = f"{db_schema}.fact_team_stats"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        category              VARCHAR NOT NULL,
        displayValue      VARCHAR,
        value             VARCHAR,
        label             VARCHAR,
        team_id           VARCHAR,
        team_abbr         VARCHAR,
        home_away         VARCHAR,
        game_id           INTEGER NOT NULL,
        ingest_timestamp  TIMESTAMP,
        source_path       VARCHAR,
        run_id            VARCHAR,
        PRIMARY KEY (game_id, team_id, category)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # player stats
    raw_tbl_name = f"{db_schema}.fact_player_stats"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        game_id           INTEGER      NOT NULL,
        athlete_id        VARCHAR      NOT NULL,
        stat_key          VARCHAR      NOT NULL,   -- the metric identifier (e.g., 'rushYds')
        team_id           VARCHAR,
        athlete_name      VARCHAR,
        category          VARCHAR,
        stat_label        VARCHAR,
        value_str         VARCHAR,                 -- raw string form from source
        ingest_timestamp  TIMESTAMP,
        source_path       VARCHAR,
        run_id            VARCHAR,
        PRIMARY KEY (game_id, athlete_id, stat_key)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # return a dictionary/json entry, its blank because are not returning data, 200 for success
    return {}, 200