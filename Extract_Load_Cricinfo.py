import os
import json
import pandas as pd 
import mysql.connector
from mysql.connector import Error
import hashlib # For generating a unique match_id

# --- Configuration ---
# Specify the path to your JSON files folder
json_folder_path = 'C:/vscode/CricSummary/cricket_data_json'  # Make sure this path is correct and has .JSON Files that needs to be loaded

# --- SQL Table Creation Queries ---
# Tables for players
CREATE_TABLE_PLAYERS = """
CREATE TABLE IF NOT EXISTS players (
    player_id VARCHAR(255) PRIMARY KEY,
    player_name VARCHAR(255) NOT NULL
)
"""

# Tables for match info
CREATE_TABLE_MATCHES = """
CREATE TABLE IF NOT EXISTS matches (
    match_id VARCHAR(255) PRIMARY KEY,
    data_version VARCHAR(50),
    created_date DATE,
    revision INT,
    balls_per_over INT,
    city VARCHAR(100),
    start_date DATE,
    end_date DATE,
    event_name VARCHAR(255),
    event_match_number INT,
    gender VARCHAR(50),
    match_type VARCHAR(50),
    match_type_number INT,
    outcome_winner VARCHAR(100),
    outcome_by_wickets INT,
    outcome_by_runs INT,
    toss_decision VARCHAR(50),
    toss_winner VARCHAR(100),
    venue_name VARCHAR(255),
    season VARCHAR(100),
    team_type VARCHAR(100)
)
"""

CREATE_TABLE_MATCH_TEAMS = """
CREATE TABLE IF NOT EXISTS match_teams (
    match_team_id INT AUTO_INCREMENT PRIMARY KEY,
    match_id VARCHAR(255) NOT NULL,
    team_name VARCHAR(100) NOT NULL,
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
)
"""

CREATE_TABLE_MATCH_SQUADS = """
CREATE TABLE IF NOT EXISTS match_squads (
    match_squad_id INT AUTO_INCREMENT PRIMARY KEY,
    match_id VARCHAR(255) NOT NULL,
    team_name VARCHAR(100) NOT NULL,
    player_id VARCHAR(255) NOT NULL,
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
)
"""

CREATE_TABLE_OFFICIALS = """
CREATE TABLE IF NOT EXISTS officials (
    official_id INT AUTO_INCREMENT PRIMARY KEY,
    match_id VARCHAR(255) NOT NULL,
    player_id VARCHAR(255) NOT NULL,
    official_role VARCHAR(100) NOT NULL,
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
)
"""

CREATE_TABLE_PLAYER_OF_MATCH = """
CREATE TABLE IF NOT EXISTS player_of_match (
    pom_id INT AUTO_INCREMENT PRIMARY KEY,
    match_id VARCHAR(255) NOT NULL,
    player_id VARCHAR(255) NOT NULL,
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
)
"""

CREATE_TABLE_INNINGS = """
CREATE TABLE IF NOT EXISTS innings (
    inning_id INT AUTO_INCREMENT PRIMARY KEY,
    match_id VARCHAR(255) NOT NULL,
    inning_number INT NOT NULL,
    team_name VARCHAR(100) NOT NULL,
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
)
"""

CREATE_TABLE_OVERS = """
CREATE TABLE IF NOT EXISTS overs (
    over_db_id INT AUTO_INCREMENT PRIMARY KEY,
    inning_id INT NOT NULL,
    over_number INT NOT NULL,
    FOREIGN KEY (inning_id) REFERENCES innings(inning_id) ON DELETE CASCADE
)
"""

CREATE_TABLE_DELIVERIES = """
CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id INT AUTO_INCREMENT PRIMARY KEY,
    over_db_id INT NOT NULL,
    delivery_number INT NOT NULL,
    batter_id VARCHAR(255),
    bowler_id VARCHAR(255),
    non_striker_id VARCHAR(255),
    runs_batter INT,
    runs_extras INT,
    runs_total INT,
    extras_wides INT DEFAULT 0,
    extras_noballs INT DEFAULT 0,
    extras_byes INT DEFAULT 0,
    extras_legbyes INT DEFAULT 0,
    extras_penalty INT DEFAULT 0,
    wicket_kind VARCHAR(100),
    wicket_player_out_id VARCHAR(255),
    wicket_fielders TEXT, -- Storing as JSON string
    FOREIGN KEY (over_db_id) REFERENCES overs(over_db_id) ON DELETE CASCADE,
    FOREIGN KEY (batter_id) REFERENCES players(player_id) ON DELETE CASCADE,
    FOREIGN KEY (bowler_id) REFERENCES players(player_id) ON DELETE CASCADE,
    FOREIGN KEY (non_striker_id) REFERENCES players(player_id) ON DELETE CASCADE,
    FOREIGN KEY (wicket_player_out_id) REFERENCES players(player_id) ON DELETE CASCADE
)
"""

# List of all table creation queries in dependency order
TABLE_CREATION_QUERIES = [
    CREATE_TABLE_PLAYERS,
    CREATE_TABLE_MATCHES,
    CREATE_TABLE_MATCH_TEAMS,
    CREATE_TABLE_MATCH_SQUADS,
    CREATE_TABLE_OFFICIALS,
    CREATE_TABLE_PLAYER_OF_MATCH,
    CREATE_TABLE_INNINGS,
    CREATE_TABLE_OVERS,
    CREATE_TABLE_DELIVERIES
]

# --- Helper Functions ---
def create_db_tables(cursor):
    """Creates all necessary tables in the database."""
    print("Attempting to create tables...")
    for query in TABLE_CREATION_QUERIES:
        try:
            cursor.execute(query)
        except Error as e:
            print(f"Error creating table: {e}\nQuery: {query.splitlines()[0]}...")
            raise # Re-raise to stop execution if table creation fails

def get_player_id(player_name, registry):
    """Retrieves player_id from the registry given player_name."""
    return registry.get(player_name)

def insert_data(json_data):
    """
    Connects to MySQL, creates tables, and inserts data from the parsed JSON.
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host="gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
            user="2zfoFbFCnKGxKRW.root",
            password='QdIF3FJ19vZSHlNs',
            port=4000,
            database="CrickerDB"
        )
        if connection.is_connected():
            cursor = connection.cursor()
            print("Successfully connected to MySQL database.")

            # Create tables
            create_db_tables(cursor)

            # --- Extract data from JSON ---
            info = json_data['info']
            meta = json_data['meta']

            # Generate a unique match_id
            match_unique_string = f"{info.get('match_type_number', '')}-{info.get('dates', [''])[0]}-{info.get('teams', ['', ''])[0]}-{info.get('teams', ['', ''])[1]}"
            match_id = hashlib.sha256(match_unique_string.encode()).hexdigest()[:20]

            # 1. Insert into 'players' table (from registry)
            print("Inserting players...")
            players_to_insert = []
            for player_name, player_uid in info.get('registry', {}).get('people', {}).items():
                players_to_insert.append((player_uid, player_name))
            if players_to_insert:
                insert_player_sql = """
                INSERT INTO players (player_id, player_name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE player_name = VALUES(player_name)
                """
                cursor.executemany(insert_player_sql, players_to_insert)
                connection.commit()
                print(f"Inserted/updated {len(players_to_insert)} players.")

            # 2. Insert into 'matches' table
            print("Inserting match details...")
            outcome_by_wickets = info.get('outcome', {}).get('by', {}).get('wickets')
            outcome_by_runs = info.get('outcome', {}).get('by', {}).get('runs')

            insert_match_sql = """
            INSERT INTO matches (
                match_id, data_version, created_date, revision, balls_per_over, city,
                start_date, end_date, event_name, event_match_number, gender,
                match_type, match_type_number, outcome_winner, outcome_by_wickets,
                outcome_by_runs, toss_decision, toss_winner, venue_name, season, team_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                data_version = VALUES(data_version), created_date = VALUES(created_date),
                revision = VALUES(revision), balls_per_over = VALUES(balls_per_over),
                city = VALUES(city), start_date = VALUES(start_date), end_date = VALUES(end_date),
                event_name = VALUES(event_name), event_match_number = VALUES(event_match_number),
                gender = VALUES(gender), match_type = VALUES(match_type),
                match_type_number = VALUES(match_type_number), outcome_winner = VALUES(outcome_winner),
                outcome_by_wickets = VALUES(outcome_by_wickets), outcome_by_runs = VALUES(outcome_by_runs),
                toss_decision = VALUES(toss_decision), toss_winner = VALUES(toss_winner),
                venue_name = VALUES(venue_name), season = VALUES(season), team_type = VALUES(team_type)
            """
            match_values = (
                match_id, meta.get('data_version'), meta.get('created'), meta.get('revision'),
                info.get('balls_per_over'), info.get('city'), info.get('dates', [''])[0], info.get('dates', [''])[-1],
                info.get('event', {}).get('name'), info.get('event', {}).get('match_number'), info.get('gender'),
                info.get('match_type'), info.get('match_type_number'), info.get('outcome', {}).get('winner'),
                outcome_by_wickets, outcome_by_runs,
                info.get('toss', {}).get('decision'), info.get('toss', {}).get('winner'),
                info.get('venue'), info.get('season'), info.get('team_type')
            )
            cursor.execute(insert_match_sql, match_values)
            connection.commit()
            print(f"Inserted/updated match: {match_id}")

            # 3. Insert into 'match_teams'
            print("Inserting match teams...")
            match_teams_to_insert = []
            for team_name in info.get('teams', []):
                match_teams_to_insert.append((match_id, team_name))
            if match_teams_to_insert:
                insert_match_team_sql = """
                INSERT INTO match_teams (match_id, team_name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE team_name = VALUES(team_name)
                """
                cursor.executemany(insert_match_team_sql, match_teams_to_insert)
                connection.commit()
                print(f"Inserted/updated {len(match_teams_to_insert)} match teams.")

            # 4. Insert into 'match_squads'
            print("Inserting match squads...")
            match_squads_to_insert = []
            for team_name, players_list in info.get('players', {}).items():
                for player_name in players_list:
                    player_uid = get_player_id(player_name, info.get('registry', {}).get('people', {}))
                    if player_uid:
                        match_squads_to_insert.append((match_id, team_name, player_uid))
                    else:
                        print(f"Warning: Player '{player_name}' not found in registry for squad.")
            if match_squads_to_insert:
                insert_match_squad_sql = """
                INSERT INTO match_squads (match_id, team_name, player_id)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE team_name = VALUES(team_name)
                """
                cursor.executemany(insert_match_squad_sql, match_squads_to_insert)
                connection.commit()
                print(f"Inserted/updated {len(match_squads_to_insert)} squad entries.")

            # 5. Insert into 'officials'
            print("Inserting officials...")
            officials_to_insert = []
            for role, names in info.get('officials', {}).items():
                for name in names:
                    player_uid = get_player_id(name, info.get('registry', {}).get('people', {}))
                    if player_uid:
                        officials_to_insert.append((match_id, player_uid, role))
                    else:
                        print(f"Warning: Official '{name}' not found in registry.")
            if officials_to_insert:
                insert_official_sql = """
                INSERT INTO officials (match_id, player_id, official_role)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE official_role = VALUES(official_role)
                """
                cursor.executemany(insert_official_sql, officials_to_insert)
                connection.commit()
                print(f"Inserted/updated {len(officials_to_insert)} officials.")

            # 6. Insert into 'player_of_match'
            print("Inserting player of match...")
            pom_to_insert = []
            for player_name in info.get('player_of_match', []):
                player_uid = get_player_id(player_name, info.get('registry', {}).get('people', {}))
                if player_uid:
                    pom_to_insert.append((match_id, player_uid))
                else:
                    print(f"Warning: Player of match '{player_name}' not found in registry.")
            if pom_to_insert:
                insert_pom_sql = """
                INSERT INTO player_of_match (match_id, player_id)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE player_id = VALUES(player_id)
                """
                cursor.executemany(insert_pom_sql, pom_to_insert)
                connection.commit()
                print(f"Inserted/updated {len(pom_to_insert)} player of match entries.")

            # 7. Loop through 'innings'
            print("Processing innings data...")
            for inning_index, inning_data in enumerate(json_data.get('innings', [])):
                inning_number = inning_index + 1
                inning_team = inning_data.get('team')

                insert_inning_sql = """
                INSERT INTO innings (match_id, inning_number, team_name)
                VALUES (%s, %s, %s)
                """
                cursor.execute(insert_inning_sql, (match_id, inning_number, inning_team))
                inning_db_id = cursor.lastrowid
                connection.commit()

                # 8. Loop through 'overs' within each inning
                for over_data in inning_data.get('overs', []):
                    over_number = over_data.get('over')

                    insert_over_sql = """
                    INSERT INTO overs (inning_id, over_number)
                    VALUES (%s, %s)
                    """
                    cursor.execute(insert_over_sql, (inning_db_id, over_number))
                    over_db_id = cursor.lastrowid
                    connection.commit()

                    # 9. Loop through 'deliveries' within each over
                    for delivery_index, delivery_data in enumerate(over_data.get('deliveries', [])):
                        delivery_number = delivery_index + 1

                        batter_name = delivery_data.get('batter')
                        bowler_name = delivery_data.get('bowler')
                        non_striker_name = delivery_data.get('non_striker')

                        batter_id = get_player_id(batter_name, info.get('registry', {}).get('people', {}))
                        bowler_id = get_player_id(bowler_name, info.get('registry', {}).get('people', {}))
                        non_striker_id = get_player_id(non_striker_name, info.get('registry', {}).get('people', {}))

                        runs = delivery_data.get('runs', {})
                        runs_batter = runs.get('batter')
                        runs_extras = runs.get('extras')
                        runs_total = runs.get('total')

                        # Handle extras
                        extras = delivery_data.get('extras', {})
                        extras_wides = extras.get('wides', 0)
                        extras_noballs = extras.get('noballs', 0)
                        extras_byes = extras.get('byes', 0)
                        extras_legbyes = extras.get('legbyes', 0)
                        extras_penalty = extras.get('penalty', 0)

                        # Handle wicket information
                        wicket_data = delivery_data.get('wicket')
                        wicket_kind = None
                        wicket_player_out_id = None
                        wicket_fielders_json = None

                        if wicket_data:
                            wicket_kind = wicket_data.get('kind')
                            player_out_name = wicket_data.get('player_out')
                            if player_out_name:
                                wicket_player_out_id = get_player_id(player_out_name, info.get('registry', {}).get('people', {}))
                            fielders = wicket_data.get('fielders')
                            if fielders:
                                # Convert fielders list to JSON string for storage
                                wicket_fielders_json = json.dumps(fielders)

                        insert_delivery_sql = """
                        INSERT INTO deliveries (
                            over_db_id, delivery_number, batter_id, bowler_id, non_striker_id,
                            runs_batter, runs_extras, runs_total,
                            extras_wides, extras_noballs, extras_byes, extras_legbyes, extras_penalty,
                            wicket_kind, wicket_player_out_id, wicket_fielders
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        delivery_values = (
                            over_db_id, delivery_number, batter_id, bowler_id, non_striker_id,
                            runs_batter, runs_extras, runs_total,
                            extras_wides, extras_noballs, extras_byes, extras_legbyes, extras_penalty,
                            wicket_kind, wicket_player_out_id, wicket_fielders_json
                        )
                        cursor.execute(insert_delivery_sql, delivery_values)
                connection.commit()

            print(f"Data for match {match_id} loaded successfully into MySQL!")

    except Error as e:
        print(f"Database error: {e}")
        if connection:
            connection.rollback()
    except FileNotFoundError:
        print(f"Error: JSON folder not found at '{json_folder_path}'.")
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from file. Check file format for errors. {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    # Ensure all required libraries are installed
    try:
        import mysql.connector
        print("All required libraries are installed. You may run the script.")
    except ImportError:
        print("Required library 'mysql.connector' is not installed.")
        print("Please run: pip install mysql-connector-python")
        exit()
        
    # Process all JSON files in the specified folder
    try:
        json_files = [f for f in os.listdir(json_folder_path) if f.endswith('.json')]
        for file_name in json_files:
            file_path = os.path.join(json_folder_path, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                insert_data(data)
    except FileNotFoundError:
        print(f"Error: The specified folder '{json_folder_path}' was not found.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")