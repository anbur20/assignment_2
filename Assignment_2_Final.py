import pandas as pd
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt
import seaborn as sns
import os
from sqlalchemy import create_engine

# --- Step 1: Connect to the SQL Database using SQLAlchemy ---
def connect_db():
    """Establishes and returns a SQLAlchemy engine for the MySQL database."""
    try:
        # Define the connection string. Using mysql.connector as the driver.
        db_connection_str = "mysql+mysqlconnector://2zfoFbFCnKGxKRW.root:QdIF3FJ19vZSHlNs@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/CrickerDB"
        engine = create_engine(db_connection_str)
        print("Successfully created SQLAlchemy engine for the database.")
        return engine
    except Exception as e:
        print(f"An error occurred while creating the database engine: {e}")
        return None

# --- Step 2: Define SQL Queries as Python strings ---
sql_queries = {
    'top_batsmen': """
        SELECT p.player_name, SUM(d.runs_batter) AS total_runs
        FROM deliveries d
        JOIN players p ON d.batter_id = p.player_id
        GROUP BY p.player_name
        ORDER BY total_runs DESC
        LIMIT 10;
    """,
    'top_bowlers': """
        SELECT p.player_name, COUNT(d.wicket_kind) AS total_wickets
        FROM deliveries d
        JOIN players p ON d.bowler_id = p.player_id
        WHERE d.wicket_kind IS NOT NULL AND d.wicket_kind != 'run out'
        GROUP BY p.player_name
        ORDER BY total_wickets DESC
        LIMIT 10;
    """,
    'dismissal_types': """
        SELECT wicket_kind, COUNT(*) AS dismissal_count
        FROM deliveries
        WHERE wicket_kind IS NOT NULL
        GROUP BY wicket_kind
        ORDER BY dismissal_count DESC;
    """,
    'matches_per_venue': """
        SELECT venue_name, COUNT(*) AS total_matches
        FROM matches
        GROUP BY venue_name
        ORDER BY total_matches DESC
        LIMIT 20;
    """,
    'toss_win_analysis': """
        SELECT
            toss_winner AS team_name,
            COUNT(*) AS toss_wins,
            (SUM(CASE WHEN toss_winner = outcome_winner THEN 1 ELSE 0 END) * 100.0) / COUNT(*) AS win_percentage_after_toss_win
        FROM matches
        WHERE toss_winner IS NOT NULL
        GROUP BY toss_winner
        ORDER BY toss_wins DESC;
    """,
    'pom_awards': """
        SELECT p.player_name, COUNT(*) AS pom_awards
        FROM player_of_match pom
        JOIN players p ON pom.player_id = p.player_id
        GROUP BY p.player_name
        ORDER BY pom_awards DESC
        LIMIT 10;
    """,
    'odi_batsmen': """
        SELECT p.player_name, SUM(d.runs_batter) AS total_runs
        FROM deliveries d
        JOIN overs o ON d.over_db_id = o.over_db_id
        JOIN innings i ON o.inning_id = i.inning_id
        JOIN matches m ON i.match_id = m.match_id
        JOIN players p ON d.batter_id = p.player_id
        WHERE m.match_type = 'ODI'
        GROUP BY p.player_name
        ORDER BY total_runs DESC
        LIMIT 5;
    """,
    'test_win_percentage': """
        SELECT
            m.outcome_winner AS team_name,
            (SUM(CASE WHEN m.outcome_winner IS NOT NULL THEN 1 ELSE 0 END) * 100.0) / COUNT(*) AS win_percentage
        FROM matches m
        WHERE m.match_type = 'Test'
        GROUP BY m.outcome_winner
        ORDER BY win_percentage DESC;
    """,
    'most_common_match_type': """
        SELECT match_type, COUNT(*) as match_count
        FROM matches
        GROUP BY match_type
        ORDER BY match_count DESC;
    """,
    'total_runs_per_inning_id': """
        SELECT
            i.inning_id,
            SUM(d.runs_batter) AS total_runs
        FROM deliveries d
        JOIN overs o ON d.over_db_id = o.over_db_id
        JOIN innings i ON o.inning_id = i.inning_id
        GROUP BY i.inning_id;
    """,
    'inning_numbers': """
        SELECT
            inning_id,
            inning_number
        FROM innings;
    """,
    'team_home_away_wins': """
        SELECT
            t.team_name,
            SUM(CASE WHEN m.outcome_winner = t.team_name THEN 1 ELSE 0 END) AS total_wins
        FROM match_teams t
        JOIN matches m ON t.match_id = m.match_id
        GROUP BY t.team_name
        ORDER BY total_wins DESC;
    """,
    'runs_per_over': """
        SELECT 
            i.inning_number,
            o.over_number,
            SUM(d.runs_batter) AS runs_scored
        FROM deliveries d
        JOIN overs o ON d.over_db_id = o.over_db_id
        JOIN innings i ON o.inning_id = i.inning_id
        GROUP BY i.inning_number, o.over_number
        ORDER BY i.inning_number, o.over_number;
    """,
    'top_wicket_takers_odi': """
        SELECT 
            p.player_name,
            COUNT(d.wicket_kind) AS total_wickets
        FROM deliveries d
        JOIN overs o ON d.over_db_id = o.over_db_id
        JOIN innings i ON o.inning_id = i.inning_id
        JOIN matches m ON i.match_id = m.match_id
        JOIN players p ON d.bowler_id = p.player_id
        WHERE m.match_type = 'ODI' AND d.wicket_kind IS NOT NULL
        GROUP BY p.player_name
        ORDER BY total_wickets DESC
        LIMIT 10;
    """,
    'century_count_per_player': """
        SELECT
            p.player_name,
            COUNT(DISTINCT i.inning_id) AS centuries
        FROM deliveries d
        JOIN overs o ON d.over_db_id = o.over_db_id
        JOIN innings i ON o.inning_id = i.inning_id
        JOIN players p ON d.batter_id = p.player_id
        GROUP BY i.inning_id, p.player_name
        HAVING SUM(d.runs_batter) >= 100
        ORDER BY centuries DESC
        LIMIT 10;
    """,
    'average_strike_rate_per_player': """
        SELECT
            p.player_name,
            (SUM(d.runs_batter) * 100.0) / COUNT(d.over_db_id) AS strike_rate
        FROM deliveries d
        JOIN players p ON d.batter_id = p.player_id
        GROUP BY p.player_name
        HAVING COUNT(d.over_db_id) >= 50
        ORDER BY strike_rate DESC
        LIMIT 10;
    """,
    'total_runs_per_season': """
        SELECT
            m.season,
            SUM(d.runs_batter) AS total_runs
        FROM deliveries d
        JOIN overs o ON d.over_db_id = o.over_db_id
        JOIN innings i ON o.inning_id = i.inning_id
        JOIN matches m ON i.match_id = m.match_id
        GROUP BY m.season
        ORDER BY m.season;
    """,
    'top_boundary_hitters': """
        SELECT 
            p.player_name,
            COUNT(*) AS total_boundaries
        FROM deliveries d
        JOIN overs o ON d.over_db_id = o.over_db_id
        JOIN innings i ON o.inning_id = i.inning_id
        JOIN players p ON d.batter_id = p.player_id
        WHERE d.runs_batter IN (4, 6)
        GROUP BY p.player_name
        ORDER BY total_boundaries DESC
        LIMIT 10;
    """,
    'toss_decision_wins': """
        SELECT
            toss_decision,
            SUM(CASE WHEN toss_winner = outcome_winner THEN 1 ELSE 0 END) AS wins
        FROM matches
        GROUP BY toss_decision;
    """
}

def get_data_from_db(query_name, engine):
    """Fetches data from the database using a given SQL query name and an active engine."""
    print(f"Executing query for: {query_name}")
    query = sql_queries[query_name]
    try:
        # pd.read_sql_query now uses the SQLAlchemy engine
        df = pd.read_sql_query(query, engine)
        return df
    except Error as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame() # Return an empty DataFrame on error

# --- Step 3: Create and run visualizations using data from the database ---
def create_visualizations(engine):
    """Generates visualizations using data from the SQL queries."""
    
    if not engine:
        print("Database connection failed. Cannot create visualizations.")
        return

    # Create the output directory if it doesn't exist
    output_dir = 'visualizations'
    os.makedirs(output_dir, exist_ok=True)
    
    print("\nGeneration of visualizations Started...")
    
    # 1. Top 10 Batsmen (Matplotlib Bar Chart)
    df_batsmen = get_data_from_db('top_batsmen', engine)
    if not df_batsmen.empty:
        plt.figure(figsize=(12, 8))
        plt.bar(df_batsmen['player_name'], df_batsmen['total_runs'], color='skyblue')
        plt.xlabel('Player Name')
        plt.ylabel('Total Runs')
        plt.title('Top 10 Batsmen by Total Runs')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '1_top_10_batsmen.png'))
        plt.show()
        print("1. Top 10 Batsmen chart created.")

    # 2. Top 10 Wicket-Takers (Seaborn Bar Plot)
    df_bowlers = get_data_from_db('top_bowlers', engine)
    if not df_bowlers.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(x='total_wickets', y='player_name', data=df_bowlers, hue='player_name', palette='viridis', legend=False)
        plt.xlabel('Total Wickets')
        plt.ylabel('Player Name')
        plt.title('Top 10 Wicket-Takers')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '2_top_10_bowlers.png'))
        plt.show()
        print("2. Top 10 Wicket-Takers chart created.")

    # 3. Most Common Types of Dismissal (Matplotlib Pie Chart)
    df_dismissals = get_data_from_db('dismissal_types', engine)
    if not df_dismissals.empty:
        plt.figure(figsize=(10, 10))
        plt.pie(df_dismissals['dismissal_count'], labels=df_dismissals['wicket_kind'], autopct='%1.1f%%', startangle=90, colors=sns.color_palette('pastel'))
        plt.title('Distribution of Wicket Types')
        plt.axis('equal')
        plt.savefig(os.path.join(output_dir, '3_wicket_types.png'))
        plt.show()
        print("3. Wicket Types chart created.")

    # 4. Matches Played per Venue (Matplotlib Horizontal Bar Chart)
    df_venues = get_data_from_db('matches_per_venue', engine)
    if not df_venues.empty:
        df_venues_sorted = df_venues.sort_values(by='total_matches', ascending=True)
        plt.figure(figsize=(12, 8))
        plt.barh(df_venues_sorted['venue_name'], df_venues_sorted['total_matches'], color='coral')
        plt.xlabel('Total Matches Played')
        plt.ylabel('Venue Name')
        plt.title('Total Matches Played per Venue')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '4_matches_per_venue.png'))
        plt.show()
        print("4. Matches per Venue chart created.")

    # 5. Toss Win vs Match Win (Seaborn Bar Plot)
    df_toss = get_data_from_db('toss_win_analysis', engine)
    if not df_toss.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(x='win_percentage_after_toss_win', y='team_name', data=df_toss, hue='team_name', palette='viridis', legend=False)
        plt.title('Win Percentage After Winning the Toss')
        plt.xlabel('Win Percentage (%)')
        plt.ylabel('Team Name')
        plt.xlim(0, 100)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '5_toss_win_percentage.png'))
        plt.show()
        print("5. Toss Win Percentage chart created.")
    
    # 6. Player of the Match Awards (Seaborn Bar Chart)
    df_pom = get_data_from_db('pom_awards', engine)
    if not df_pom.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(x='pom_awards', y='player_name', data=df_pom, hue='player_name', palette='coolwarm', legend=False)
        plt.xlabel('Number of Player of the Match Awards')
        plt.ylabel('Player Name')
        plt.title('Most "Player of the Match" Awards')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '6_pom_awards.png'))
        plt.show()
        print("6. Player of the Match awards chart created.")

    # 7. Top 5 Run-Scorers in ODI matches (Matplotlib Bar Chart)
    df_odi_batsmen = get_data_from_db('odi_batsmen', engine)
    if not df_odi_batsmen.empty:
        plt.figure(figsize=(12, 8))
        plt.bar(df_odi_batsmen['player_name'], df_odi_batsmen['total_runs'], color='purple')
        plt.xlabel('Player Name')
        plt.ylabel('Total Runs')
        plt.title('Top 5 Run-Scorers in ODI Matches')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '7_top_5_odi_batsmen.png'))
        plt.show()
        print("7. Top 5 ODI Batsmen chart created.")
    
    # 8. Team Win Percentage in Test Cricket (Seaborn Bar Chart)
    df_test_win = get_data_from_db('test_win_percentage', engine)
    if not df_test_win.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(x='win_percentage', y='team_name', data=df_test_win, hue='team_name', palette='rocket', legend=False)
        plt.xlabel('Win Percentage (%)')
        plt.ylabel('Team Name')
        plt.title('Team Win Percentage in Test Cricket')
        plt.xlim(0, 100)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '8_test_win_percentage.png'))
        plt.show()
        print("8. Test Win Percentage chart created.")
    
    # 9. Most Common Match Types
    df_match_types = get_data_from_db('most_common_match_type', engine)
    if not df_match_types.empty:
        plt.figure(figsize=(8, 8))
        plt.pie(df_match_types['match_count'], labels=df_match_types['match_type'], autopct='%1.1f%%', startangle=90, colors=sns.color_palette('pastel'))
        plt.title('Distribution of Match Types')
        plt.axis('equal')
        plt.savefig(os.path.join(output_dir, '9_match_type_distribution.png'))
        plt.show()
        print("9. Match Type Distribution chart created.")

    # 10. Average Score by Inning.
    # The aggregation is achieved in two steps as mentioned below:
    # 1. Fetch total runs per individual inning (a valid query).
    # 2. Use pandas to calculate the average score per inning number.
    df_runs_per_inning_id = get_data_from_db('total_runs_per_inning_id', engine)
    df_inning_numbers = get_data_from_db('inning_numbers', engine)

    if not df_runs_per_inning_id.empty and not df_inning_numbers.empty:
        # Merge the two dataframes to link total runs with inning numbers
        df_combined = pd.merge(df_runs_per_inning_id, df_inning_numbers, on='inning_id', how='inner')

        # Now calculate the average score for each inning number using pandas
        df_avg_score = df_combined.groupby('inning_number')['total_runs'].mean().reset_index()

        plt.figure(figsize=(10, 6))
        plt.bar(df_avg_score['inning_number'], df_avg_score['total_runs'], color='lightgreen')
        plt.xlabel('Inning Number')
        plt.ylabel('Average Score')
        plt.title('Average Score by Inning')
        plt.xticks(df_avg_score['inning_number'])
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '10_average_score_by_inning.png'))
        #plt.show()
        print("10. Average Score by Inning chart created.")
    
    # 11. Total Wins per Team.
    df_home_away = get_data_from_db('team_home_away_wins', engine)
    if not df_home_away.empty:
        plt.figure(figsize=(12, 8))
        # FIX: Added `hue` and `legend=False` to resolve deprecation warning
        sns.barplot(x='total_wins', y='team_name', data=df_home_away, hue='team_name', palette='Paired', legend=False)
        plt.xlabel('Number of Wins')
        plt.ylabel('Team Name')
        plt.title('Total Wins per Team')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '11_total_wins_per_team.png'))
        #plt.show()
        print("11. Total Wins per Team chart created.")

    # 12. Runs per Over
    df_runs_over = get_data_from_db('runs_per_over', engine)
    if not df_runs_over.empty:
        plt.figure(figsize=(15, 8))
        sns.lineplot(data=df_runs_over, x='over_number', y='runs_scored', hue='inning_number', style='inning_number', markers=True)
        plt.xlabel('Over Number')
        plt.ylabel('Runs Scored')
        plt.title('Runs Scored Per Over by Inning')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '12_runs_per_over.png'))
        #plt.show()
        print("12. Runs per Over chart created.")

    # 13. Top Wicket Takers in ODIs
    df_odi_bowlers = get_data_from_db('top_wicket_takers_odi', engine)
    if not df_odi_bowlers.empty:
        plt.figure(figsize=(12, 8))
        # FIX: Added `hue` and `legend=False` to resolve deprecation warning
        sns.barplot(x='total_wickets', y='player_name', data=df_odi_bowlers, hue='player_name', palette='crest', legend=False)
        plt.xlabel('Total Wickets')
        plt.ylabel('Player Name')
        plt.title('Top 10 Wicket Takers in ODI Matches')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '13_top_odi_bowlers.png'))
        #plt.show()
        print("13. Top Wicket Takers in ODIs chart created.")

    # 14. Century Count per Player.
    df_centuries = get_data_from_db('century_count_per_player', engine)
    if not df_centuries.empty:
        plt.figure(figsize=(12, 8))
        # FIX: Added `hue` and `legend=False` to resolve deprecation warning
        sns.barplot(x='centuries', y='player_name', data=df_centuries, hue='player_name', palette='mako', legend=False)
        plt.xlabel('Number of Centuries')
        plt.ylabel('Player Name')
        plt.title('Number of Centuries per Player')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '14_centuries_per_player.png'))
        plt.show()
        print("14. Century Count per Player chart created.")

    # 15. Average Strike Rate per Player.
    df_strike_rate = get_data_from_db('average_strike_rate_per_player', engine)
    if not df_strike_rate.empty:
        plt.figure(figsize=(12, 8))
        # FIX: Added `hue` and `legend=False` to resolve deprecation warning
        sns.barplot(x='strike_rate', y='player_name', data=df_strike_rate, hue='player_name', palette='rocket', legend=False)
        plt.xlabel('Average Strike Rate')
        plt.ylabel('Player Name')
        plt.title('Top 10 Players by Average Strike Rate')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '15_strike_rate.png'))
        plt.show()
        print("15. Average Strike Rate per Player chart created.")

    # 16. Total Runs per Season
    df_runs_season = get_data_from_db('total_runs_per_season', engine)
    if not df_runs_season.empty:
        plt.figure(figsize=(12, 8))
        plt.plot(df_runs_season['season'], df_runs_season['total_runs'], marker='o', linestyle='-', color='purple')
        plt.xlabel('Season')
        plt.ylabel('Total Runs Scored')
        plt.title('Total Runs Scored Per Season')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '16_runs_per_season.png'))
        plt.show()
        print("16. Total Runs per Season chart created.")

    # 17. Top Boundary Hitters
    df_boundaries = get_data_from_db('top_boundary_hitters', engine)
    if not df_boundaries.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(x='total_boundaries', y='player_name', data=df_boundaries, hue='player_name', palette='crest', legend=False)
        plt.xlabel('Total Boundaries (4s & 6s)')
        plt.ylabel('Player Name')
        plt.title('Top 10 Boundary Hitters')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '17_top_boundary_hitters.png'))
        plt.show()
        print("17. Top Boundary Hitters chart created.")

    # 18. Toss Decision vs. Wins
    df_toss_decision = get_data_from_db('toss_decision_wins', engine)
    if not df_toss_decision.empty:
        plt.figure(figsize=(8, 8))
        plt.pie(df_toss_decision['wins'], labels=df_toss_decision['toss_decision'], autopct='%1.1f%%', startangle=90, colors=sns.color_palette('pastel'))
        plt.title('Wins by Toss Decision')
        plt.axis('equal')
        plt.savefig(os.path.join(output_dir, '18_toss_decision_wins.png'))
        plt.show()
        print("18. Toss Decision vs. Wins chart created.")

if __name__ == '__main__':
    engine = connect_db()
    if engine:
        create_visualizations(engine)
        print("\nAll visualizations created successfully.")
