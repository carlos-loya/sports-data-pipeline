# Data Dictionary

Complete reference for every table and column in the sports-data-pipeline, organized by medallion layer.

> **How to read this document**
>
> - **Source** = field comes directly from the upstream API/scrape
> - **Derived** = field is computed during transformation
> - **Metadata** = field added by the pipeline itself (e.g. timestamps)
> - **Nullable** columns may contain `NULL` / `NaN`; non-nullable columns are guaranteed present

---

## Storage Layout

| Layer | Format | Path Pattern |
|-------|--------|-------------|
| Bronze | Parquet | `data/bronze/{sport}/season={s}/date={d}/{type}.parquet` |
| Silver | Parquet | `data/silver/{sport}/{type}.parquet` |
| Gold | DuckDB | `data/gold/sports_analytics.duckdb` (schema: `gold`) |

---

## Data Sources

| Sport | Source | Library | Rate Limit |
|-------|--------|---------|------------|
| NFL | [nflverse](https://github.com/nflverse) | `nflreadpy` | 20 req/min |
| NBA | [stats.nba.com](https://stats.nba.com) | `nba_api` | 30 req/min |
| Soccer | [FBref](https://fbref.com) | `requests` + `beautifulsoup4` | 8 req/min |

---

## Bronze Layer (Raw)

Bronze data is the unmodified output of extractors, written as dated Parquet files. Every bronze table includes an `extract_timestamp` metadata column recording when the extraction ran.

### NFL

#### `games.parquet` — Game schedule and scores

Extracted from nflreadpy schedule data. One row per game.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `extract_timestamp` | datetime | No | Metadata | When this extraction ran |
| `season` | int | No | Source | NFL season year (e.g. `2024`) |
| `week` | int | No | Source | Week number (1-18 regular season, higher for playoffs) |
| `game_id` | str | No | Source | Unique game identifier (e.g. `2024_01_KC_BAL`) |
| `gameday` | date | Yes | Source | Date the game was played |
| `gametime` | str | Yes | Source | Kickoff time (e.g. `20:20`) |
| `home_team` | str | No | Source | Home team abbreviation (e.g. `BAL`, `KC`) |
| `away_team` | str | No | Source | Away team abbreviation |
| `home_score` | int | Yes | Source | Home team final score. `NULL` if game hasn't been played |
| `away_score` | int | Yes | Source | Away team final score. `NULL` if game hasn't been played |
| `overtime` | bool | No | Source | Whether the game went to overtime |
| `game_type` | str | Yes | Source | `REG` (regular season), `POST` (playoff), `PRE` (preseason) |
| `location` | str | Yes | Source | Game location type |
| `stadium` | str | Yes | Source | Stadium name |
| `roof` | str | Yes | Source | `outdoors`, `dome`, or `retractable` |
| `surface` | str | Yes | Source | `grass` or `astroturf` |
| `spread_line` | float | Yes | Source | Vegas spread line (negative = home favored) |
| `total_line` | float | Yes | Source | Vegas over/under total |
| `result` | int | Yes | Source | Home score minus away score |

#### `players.parquet` — Player weekly stats

Extracted from nflreadpy weekly player stats. One row per player per week.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `extract_timestamp` | datetime | No | Metadata | When this extraction ran |
| `season` | int | No | Source | NFL season year |
| `week` | int | No | Source | Week number |
| `player_id` | str | No | Source | Unique player identifier (e.g. `00-0036442`) |
| `player_name` | str | No | Source | Abbreviated name (e.g. `P.Mahomes`) |
| `player_display_name` | str | No | Source | Full name (e.g. `Patrick Mahomes`) |
| `team` | str | Yes | Source | Team abbreviation (from `recent_team`) |
| `position` | str | Yes | Source | Position (e.g. `QB`, `RB`, `WR`) |
| **Passing** |
| `completions` | int | Yes | Source | Completed passes |
| `passing_attempts` | int | Yes | Source | Pass attempts |
| `passing_yards` | float | Yes | Source | Passing yards |
| `passing_tds` | int | Yes | Source | Passing touchdowns |
| `interceptions` | int | Yes | Source | Interceptions thrown |
| `sacks` | int | Yes | Source | Times sacked |
| `passing_air_yards` | float | Yes | Source | Air yards on pass attempts |
| `passing_yards_after_catch` | float | Yes | Source | Yards after catch by receivers |
| `passing_2pt_conversions` | int | Yes | Source | 2-point conversions (passing) |
| **Rushing** |
| `carries` | int | Yes | Source | Rush attempts |
| `rushing_yards` | float | Yes | Source | Rushing yards |
| `rushing_tds` | int | Yes | Source | Rushing touchdowns |
| `rushing_fumbles` | int | Yes | Source | Fumbles on rush plays |
| `rushing_2pt_conversions` | int | Yes | Source | 2-point conversions (rushing) |
| **Receiving** |
| `receptions` | int | Yes | Source | Receptions (catches) |
| `targets` | int | Yes | Source | Times targeted by a pass |
| `receiving_yards` | float | Yes | Source | Receiving yards |
| `receiving_tds` | int | Yes | Source | Receiving touchdowns |
| `receiving_air_yards` | float | Yes | Source | Air yards on targets |
| `receiving_yards_after_catch` | float | Yes | Source | Yards after catch |
| `receiving_fumbles` | int | Yes | Source | Fumbles after reception |
| `receiving_2pt_conversions` | int | Yes | Source | 2-point conversions (receiving) |
| **Fantasy** |
| `fantasy_points` | float | Yes | Source | PPR fantasy points |

#### `team_stats.parquet` — Team season stats (aggregated)

Derived by aggregating completed games from the schedule. One row per team per season.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `extract_timestamp` | datetime | No | Metadata | When this extraction ran |
| `season` | int | No | Source | NFL season year |
| `team` | str | No | Source | Team abbreviation |
| `games_played` | int | No | Derived | Total completed games |
| `wins` | int | No | Derived | Total wins |
| `losses` | int | No | Derived | Total losses |
| `ties` | int | No | Derived | Total ties |
| `points_for` | int | No | Derived | Total points scored |
| `points_against` | int | No | Derived | Total points allowed |
| `point_differential` | int | No | Derived | `points_for - points_against` |
| `home_wins` | int | No | Derived | Wins at home |
| `home_losses` | int | No | Derived | Losses at home |
| `away_wins` | int | No | Derived | Wins on the road |
| `away_losses` | int | No | Derived | Losses on the road |

---

### NBA

#### `games.parquet` — Game results

Extracted from nba_api league game log. Home/away rows are paired by `GAME_ID`. One row per game.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `extract_timestamp` | datetime | No | Metadata | When this extraction ran |
| `season` | str | No | Source | NBA season string (e.g. `2024-25`) |
| `game_id` | str | No | Source | Unique game identifier (e.g. `0022400001`) |
| `game_date` | date | No | Source | Date the game was played |
| `home_team_id` | int | No | Source | NBA team ID for home team |
| `home_team_name` | str | No | Source | Home team full name |
| `away_team_id` | int | No | Source | NBA team ID for away team |
| `away_team_name` | str | No | Source | Away team full name |
| `home_score` | int | Yes | Source | Home team final score |
| `away_score` | int | Yes | Source | Away team final score |
| `status` | str | Yes | Source | Game status (e.g. `Final`) |

#### `players.parquet` — Player game logs

Extracted from nba_api player game log. One row per player per game.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `extract_timestamp` | datetime | No | Metadata | When this extraction ran |
| `season` | str | No | Source | NBA season string |
| `game_id` | str | No | Source | Game identifier |
| `game_date` | date | No | Source | Date of the game |
| `player_id` | int | No | Source | NBA player ID |
| `player_name` | str | No | Source | Player full name |
| `team_id` | int | No | Source | NBA team ID |
| `team_name` | str | No | Source | Team full name |
| `is_home` | bool | No | Derived | `true` if MATCHUP contains `vs.` |
| `minutes` | float | Yes | Source | Minutes played |
| `points` | int | Yes | Source | Points scored |
| `rebounds` | int | Yes | Source | Total rebounds |
| `assists` | int | Yes | Source | Assists |
| `steals` | int | Yes | Source | Steals |
| `blocks` | int | Yes | Source | Blocks |
| `turnovers` | int | Yes | Source | Turnovers |
| `field_goals_made` | int | Yes | Source | Field goals made (FGM) |
| `field_goals_attempted` | int | Yes | Source | Field goals attempted (FGA) |
| `three_pointers_made` | int | Yes | Source | 3-pointers made (FG3M) |
| `three_pointers_attempted` | int | Yes | Source | 3-pointers attempted (FG3A) |
| `free_throws_made` | int | Yes | Source | Free throws made (FTM) |
| `free_throws_attempted` | int | Yes | Source | Free throws attempted (FTA) |
| `plus_minus` | float | Yes | Source | Plus/minus while on court |

#### `team_stats.parquet` — Team advanced metrics

Extracted from nba_api team estimated metrics. One row per team per season.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `extract_timestamp` | datetime | No | Metadata | When this extraction ran |
| `season` | str | No | Source | NBA season string |
| `team_id` | int | No | Source | NBA team ID |
| `team_name` | str | No | Source | Team full name |
| `games_played` | int | No | Source | Games played (GP) |
| `wins` | int | No | Source | Total wins |
| `losses` | int | No | Source | Total losses |
| `offensive_rating` | float | Yes | Source | Estimated offensive rating (points per 100 possessions) |
| `defensive_rating` | float | Yes | Source | Estimated defensive rating (points allowed per 100 possessions) |
| `net_rating` | float | Yes | Source | Estimated net rating (offensive - defensive) |
| `pace` | float | Yes | Source | Estimated pace (possessions per 48 minutes) |

---

### Soccer

#### `matches.parquet` — Match results

Extracted from FBref schedule pages. Only completed matches (with scores) are included. One row per match.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `extract_timestamp` | datetime | No | Metadata | When this extraction ran |
| `season` | str | No | Source | Season string (e.g. `2024-2025`) |
| `league` | str | No | Source | League name (e.g. `Premier League`) |
| `match_date` | date | No | Source | Date the match was played |
| `home_team` | str | No | Source | Home team name |
| `away_team` | str | No | Source | Away team name |
| `home_goals` | int | Yes | Source | Goals scored by home team |
| `away_goals` | int | Yes | Source | Goals scored by away team |
| `home_xg` | float | Yes | Source | Home expected goals (xG) |
| `away_xg` | float | Yes | Source | Away expected goals (xG) |
| `venue` | str | Yes | Source | Stadium/venue name |
| `referee` | str | Yes | Source | Match referee |
| `attendance` | int | Yes | Source | Match attendance |
| `match_url` | str | Yes | Source | FBref match report URL |

#### `players.parquet` — Player match stats

Extracted from FBref match report pages. One row per player per match.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `extract_timestamp` | datetime | No | Metadata | When this extraction ran |
| `season` | str | No | Source | Season string |
| `league` | str | No | Source | League name |
| `match_date` | date | Yes | Source | Date of the match |
| `player_name` | str | No | Source | Player name |
| `team` | str | No | Source | Player's team |
| `opponent` | str | No | Source | Opposing team |
| `is_home` | bool | No | Derived | Whether the player's team was at home |
| `minutes` | int | Yes | Source | Minutes played |
| `goals` | int | Yes | Source | Goals scored |
| `assists` | int | Yes | Source | Assists |
| `shots` | int | Yes | Source | Total shots |
| `shots_on_target` | int | Yes | Source | Shots on target |
| `xg` | float | Yes | Source | Expected goals |
| `xa` | float | Yes | Source | Expected assists |

#### `team_stats.parquet` — Team season stats

Extracted from FBref league standings pages. One row per team per season per league.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `extract_timestamp` | datetime | No | Metadata | When this extraction ran |
| `season` | str | No | Source | Season string |
| `league` | str | No | Source | League name |
| `team` | str | No | Source | Team name (from FBref "Squad" column) |
| `matches_played` | int | No | Source | Matches played |
| `wins` | int | No | Source | Wins |
| `draws` | int | No | Source | Draws |
| `losses` | int | No | Source | Losses |
| `goals_for` | int | No | Source | Goals scored |
| `goals_against` | int | No | Source | Goals conceded |
| `goal_difference` | int | No | Source | `goals_for - goals_against` |
| `points` | int | No | Source | League points (3 per win, 1 per draw) |
| `xg_for` | float | Yes | Source | Expected goals for |
| `xg_against` | float | Yes | Source | Expected goals against |

---

## Silver Layer (Cleaned & Normalized)

Silver data applies transformations to bronze: team name normalization, deduplication, type coercion, and derived fields. The `extract_timestamp` column is removed — silver data represents the current best version of the data.

### Transformations Applied

| Transformation | Description |
|---------------|-------------|
| **Team normalization** | Abbreviations mapped to canonical names (e.g. `KC` -> `Kansas City Chiefs`, `BAL` -> `Baltimore Ravens`) |
| **Player name normalization** | Whitespace stripped; NFL uses `player_display_name` over abbreviated `player_name` |
| **Deduplication** | NFL games by `game_id`; NFL players by `(player_id, season, week)`; Soccer matches by `match_id` |
| **Type coercion** | Stat columns coerced to numeric; nullable stats filled with `0` or `0.0` |
| **Derived fields** | Calculated from existing columns (see tables below) |

---

### NFL

#### Silver NFL Games

Deduplicated by `game_id`. Team abbreviations normalized to full names.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `game_id` | str | No | Bronze | Unique game identifier |
| `season` | int | No | Bronze | NFL season year |
| `week` | int | No | Bronze | Week number |
| `game_date` | date | Yes | Bronze | Date played (cast from `gameday`) |
| `home_team` | str | No | Bronze | Home team full name (normalized) |
| `away_team` | str | No | Bronze | Away team full name (normalized) |
| `home_score` | int | Yes | Bronze | Home team final score |
| `away_score` | int | Yes | Bronze | Away team final score |
| `home_win` | bool | Yes | **Derived** | `home_score > away_score`. `NULL` if either score is missing |
| `total_points` | int | Yes | **Derived** | `home_score + away_score`. `NULL` if either score is missing |
| `overtime` | bool | No | Bronze | Whether the game went to overtime |
| `game_type` | str | Yes | Bronze | Game type (`REG`, `POST`, `PRE`) |
| `stadium` | str | Yes | Bronze | Stadium name |

#### Silver NFL Players

Deduplicated by `(player_id, season, week)`. All stat columns default to `0`/`0.0`.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `player_id` | str | No | Bronze | Unique player identifier |
| `season` | int | No | Bronze | NFL season year |
| `week` | int | No | Bronze | Week number |
| `player_name` | str | No | Bronze | Full display name (normalized) |
| `team` | str | No | Bronze | Team full name (normalized) |
| `position` | str | Yes | Bronze | Position |
| `completions` | int | No | Bronze | Completed passes |
| `passing_attempts` | int | No | Bronze | Pass attempts |
| `passing_yards` | float | No | Bronze | Passing yards |
| `passing_tds` | int | No | Bronze | Passing touchdowns |
| `interceptions` | int | No | Bronze | Interceptions thrown |
| `sacks` | int | No | Bronze | Times sacked |
| `carries` | int | No | Bronze | Rush attempts |
| `rushing_yards` | float | No | Bronze | Rushing yards |
| `rushing_tds` | int | No | Bronze | Rushing touchdowns |
| `rushing_fumbles` | int | No | Bronze | Fumbles on rushes |
| `receptions` | int | No | Bronze | Receptions |
| `targets` | int | No | Bronze | Times targeted |
| `receiving_yards` | float | No | Bronze | Receiving yards |
| `receiving_tds` | int | No | Bronze | Receiving touchdowns |
| `fantasy_points` | float | No | Bronze | PPR fantasy points |
| `total_yards` | float | No | **Derived** | `passing_yards + rushing_yards + receiving_yards` |
| `total_tds` | int | No | **Derived** | `passing_tds + rushing_tds + receiving_tds` |

---

### NBA

#### Silver NBA Games

Deduplicated by `game_id`. Team names normalized.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `game_id` | str | No | Bronze | Unique game identifier |
| `season` | str | No | Bronze | NBA season string (e.g. `2024-25`) |
| `game_date` | date | No | Bronze | Date played |
| `home_team_id` | int | No | Bronze | NBA home team ID |
| `home_team` | str | No | Bronze | Home team full name (normalized) |
| `away_team_id` | int | No | Bronze | NBA away team ID |
| `away_team` | str | No | Bronze | Away team full name (normalized) |
| `home_score` | int | Yes | Bronze | Home team final score |
| `away_score` | int | Yes | Bronze | Away team final score |
| `home_win` | bool | Yes | **Derived** | `home_score > away_score`. `NULL` if either score is missing |
| `total_points` | int | Yes | **Derived** | `home_score + away_score`. `NULL` if either score is missing |

#### Silver NBA Players

All stat columns default to `0`/`0.0`. Shooting percentages derived from made/attempted.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `game_id` | str | No | Bronze | Game identifier |
| `season` | str | No | Bronze | NBA season string |
| `game_date` | date | No | Bronze | Date played |
| `player_id` | int | No | Bronze | NBA player ID |
| `player_name` | str | No | Bronze | Player full name (stripped) |
| `team_id` | int | No | Bronze | NBA team ID |
| `team` | str | No | Bronze | Team full name (normalized) |
| `is_home` | bool | No | Bronze | Whether player's team was home |
| `minutes` | float | No | Bronze | Minutes played |
| `points` | int | No | Bronze | Points scored |
| `rebounds` | int | No | Bronze | Total rebounds |
| `assists` | int | No | Bronze | Assists |
| `steals` | int | No | Bronze | Steals |
| `blocks` | int | No | Bronze | Blocks |
| `turnovers` | int | No | Bronze | Turnovers |
| `field_goal_pct` | float | Yes | **Derived** | `FGM / FGA` (NULL if `FGA = 0`) |
| `three_point_pct` | float | Yes | **Derived** | `FG3M / FG3A` (NULL if `FG3A = 0`) |
| `free_throw_pct` | float | Yes | **Derived** | `FTM / FTA` (NULL if `FTA = 0`) |
| `plus_minus` | float | No | Bronze | Plus/minus while on court |

---

### Soccer

#### Silver Soccer Matches

Deduplicated by `match_id`. Team names normalized. Match result derived from goals.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `match_id` | str | No | **Derived** | MD5 hash (first 12 chars) of `season + match_date + home_team + away_team` |
| `season` | str | No | Bronze | Season string |
| `league` | str | No | Bronze | League name |
| `match_date` | date | No | Bronze | Date played |
| `home_team` | str | No | Bronze | Home team (normalized) |
| `away_team` | str | No | Bronze | Away team (normalized) |
| `home_goals` | int | Yes | Bronze | Home goals |
| `away_goals` | int | Yes | Bronze | Away goals |
| `home_xg` | float | Yes | Bronze | Home expected goals |
| `away_xg` | float | Yes | Bronze | Away expected goals |
| `result` | str | Yes | **Derived** | `H` (home win), `A` (away win), `D` (draw). `NULL` if goals missing |
| `venue` | str | Yes | Bronze | Stadium/venue |

#### Silver Soccer Players

All stat columns default to `0`/`0.0`.

| Column | Type | Nullable | Origin | Description |
|--------|------|----------|--------|-------------|
| `match_id` | str | Yes | Bronze | Match identifier |
| `season` | str | No | Bronze | Season string |
| `league` | str | No | Bronze | League name |
| `match_date` | date | No | Bronze | Date played |
| `player_name` | str | No | Bronze | Player name (stripped) |
| `team` | str | No | Bronze | Player's team (normalized) |
| `opponent` | str | No | Bronze | Opposing team (normalized) |
| `is_home` | bool | No | Bronze | Whether player's team was home |
| `minutes` | int | No | Bronze | Minutes played |
| `goals` | int | No | Bronze | Goals scored |
| `assists` | int | No | Bronze | Assists |
| `xg` | float | No | Bronze | Expected goals |
| `xa` | float | No | Bronze | Expected assists |
| `shots` | int | No | Bronze | Total shots |
| `shots_on_target` | int | No | Bronze | Shots on target |
| `key_passes` | int | No | Bronze | Key passes |
| `tackles` | int | No | Bronze | Tackles |
| `interceptions` | int | No | Bronze | Interceptions |

---

## Gold Layer (Analytics — DuckDB)

Gold tables live in the `gold` schema of the DuckDB database. They are built from silver data and include pre-computed analytics like rolling form, head-to-head records, and team summaries.

### Tables

#### `gold.soccer_matches`

Direct load of silver soccer matches into DuckDB.

| Column | SQL Type | Nullable | Description |
|--------|----------|----------|-------------|
| `match_id` | VARCHAR | No (PK) | Unique match identifier |
| `season` | VARCHAR | Yes | Season string |
| `league` | VARCHAR | Yes | League name |
| `match_date` | DATE | Yes | Date played |
| `home_team` | VARCHAR | No | Home team |
| `away_team` | VARCHAR | No | Away team |
| `home_goals` | INTEGER | Yes | Home goals |
| `away_goals` | INTEGER | Yes | Away goals |
| `home_xg` | DOUBLE | Yes | Home expected goals |
| `away_xg` | DOUBLE | Yes | Away expected goals |
| `result` | VARCHAR | Yes | `H`, `A`, or `D` |
| `venue` | VARCHAR | Yes | Venue |

#### `gold.soccer_player_matches`

Direct load of silver soccer player match data.

| Column | SQL Type | Default | Description |
|--------|----------|---------|-------------|
| `match_id` | VARCHAR | | Match identifier |
| `season` | VARCHAR | | Season string |
| `league` | VARCHAR | | League name |
| `match_date` | DATE | | Date played |
| `player_name` | VARCHAR | | Player name |
| `team` | VARCHAR | | Player's team |
| `opponent` | VARCHAR | | Opposing team |
| `is_home` | BOOLEAN | | Home team flag |
| `minutes` | INTEGER | `0` | Minutes played |
| `goals` | INTEGER | `0` | Goals scored |
| `assists` | INTEGER | `0` | Assists |
| `xg` | DOUBLE | `0` | Expected goals |
| `xa` | DOUBLE | `0` | Expected assists |
| `shots` | INTEGER | `0` | Shots |
| `shots_on_target` | INTEGER | `0` | Shots on target |
| `key_passes` | INTEGER | `0` | Key passes |
| `tackles` | INTEGER | `0` | Tackles |
| `interceptions` | INTEGER | `0` | Interceptions |

#### `gold.nba_games`

Direct load of silver NBA games into DuckDB.

| Column | SQL Type | Nullable | Description |
|--------|----------|----------|-------------|
| `game_id` | VARCHAR | No (PK) | Unique game identifier |
| `season` | VARCHAR | Yes | NBA season string |
| `game_date` | DATE | Yes | Date played |
| `home_team_id` | INTEGER | No | Home team ID |
| `home_team` | VARCHAR | No | Home team name |
| `away_team_id` | INTEGER | No | Away team ID |
| `away_team` | VARCHAR | No | Away team name |
| `home_score` | INTEGER | Yes | Home score |
| `away_score` | INTEGER | Yes | Away score |
| `home_win` | BOOLEAN | Yes | Home team won |
| `total_points` | INTEGER | Yes | Combined score |

#### `gold.nfl_games`

Direct load of silver NFL games into DuckDB.

| Column | SQL Type | Nullable | Description |
|--------|----------|----------|-------------|
| `game_id` | VARCHAR | No (PK) | Unique game identifier (e.g. `2024_01_KC_BAL`) |
| `season` | INTEGER | Yes | NFL season year |
| `week` | INTEGER | Yes | Week number |
| `game_date` | DATE | Yes | Date played |
| `home_team` | VARCHAR | No | Home team full name |
| `away_team` | VARCHAR | No | Away team full name |
| `home_score` | INTEGER | Yes | Home team final score |
| `away_score` | INTEGER | Yes | Away team final score |
| `overtime` | BOOLEAN | Yes | Whether game went to overtime |
| `game_type` | VARCHAR | Yes | `REG`, `POST`, or `PRE` |
| `stadium` | VARCHAR | Yes | Stadium name |
| `home_win` | BOOLEAN | Yes | Home team won |
| `total_points` | INTEGER | Yes | Combined score |

#### `gold.nba_player_games`

Direct load of silver NBA player game data.

| Column | SQL Type | Default | Description |
|--------|----------|---------|-------------|
| `game_id` | VARCHAR | | Game identifier |
| `season` | VARCHAR | | NBA season string |
| `game_date` | DATE | | Date played |
| `player_id` | INTEGER | | NBA player ID |
| `player_name` | VARCHAR | | Player name |
| `team_id` | INTEGER | | Team ID |
| `team` | VARCHAR | | Team name |
| `is_home` | BOOLEAN | | Home team flag |
| `minutes` | DOUBLE | `0` | Minutes played |
| `points` | INTEGER | `0` | Points scored |
| `rebounds` | INTEGER | `0` | Rebounds |
| `assists` | INTEGER | `0` | Assists |
| `steals` | INTEGER | `0` | Steals |
| `blocks` | INTEGER | `0` | Blocks |
| `turnovers` | INTEGER | `0` | Turnovers |
| `field_goal_pct` | DOUBLE | | FG% (NULL if no attempts) |
| `three_point_pct` | DOUBLE | | 3PT% (NULL if no attempts) |
| `free_throw_pct` | DOUBLE | | FT% (NULL if no attempts) |
| `plus_minus` | DOUBLE | `0` | Plus/minus |

---

### Computed Analytics Tables

These tables are rebuilt by `GoldBuilder` and contain pre-aggregated analytics.

#### `gold.soccer_team_form` — Rolling team form (last 5 matches)

One row per team per league. Rebuilt on each pipeline run.

| Column | Type | Description |
|--------|------|-------------|
| `team` | str | Team name |
| `league` | str | League name |
| `as_of_date` | date | Date of the most recent match included |
| `last_n_matches` | int | Window size (default: `5`) |
| `wins` | int | Wins in window |
| `draws` | int | Draws in window |
| `losses` | int | Losses in window |
| `goals_scored` | int | Total goals scored in window |
| `goals_conceded` | int | Total goals conceded in window |
| `xg_for` | float | Sum of expected goals for |
| `xg_against` | float | Sum of expected goals against |
| `points` | int | Points in window (`wins * 3 + draws * 1`) |
| `form_string` | str | Recent results, newest first (e.g. `WWDLW`) |

#### `gold.soccer_h2h` — Head-to-head records

One row per team pair per league. Teams are ordered alphabetically (`team_a < team_b`).

| Column | Type | Description |
|--------|------|-------------|
| `team_a` | str | First team (alphabetically) |
| `team_b` | str | Second team (alphabetically) |
| `league` | str | League name |
| `total_matches` | int | Total meetings |
| `team_a_wins` | int | Wins for team_a |
| `draws` | int | Drawn matches |
| `team_b_wins` | int | Wins for team_b |
| `team_a_goals` | int | Total goals by team_a |
| `team_b_goals` | int | Total goals by team_b |

#### `gold.nba_team_form` — Rolling team form (last 10 games)

One row per team. Rebuilt on each pipeline run.

| Column | Type | Description |
|--------|------|-------------|
| `team` | str | Team name |
| `team_id` | int | NBA team ID |
| `as_of_date` | date | Date of the most recent game included |
| `last_n_games` | int | Window size (default: `10`) |
| `wins` | int | Wins in window |
| `losses` | int | Losses in window |
| `avg_points_scored` | float | Average points scored per game |
| `avg_points_allowed` | float | Average points allowed per game |
| `avg_point_diff` | float | Average point differential per game |

---

### Views

#### `gold.v_soccer_recent_form`

Home perspective summary of soccer matches, grouped by team/season/league.

| Column | Type | Description |
|--------|------|-------------|
| `team` | str | Team name (home only) |
| `season` | str | Season string |
| `league` | str | League name |
| `matches` | int | Home matches played |
| `home_points` | int | Points earned at home (`W=3, D=1, L=0`) |
| `goals_scored` | int | Home goals scored |
| `goals_conceded` | int | Home goals conceded |

#### `gold.v_nba_team_summary`

Home perspective summary of NBA games, grouped by team/season.

| Column | Type | Description |
|--------|------|-------------|
| `team` | str | Team name (home only) |
| `season` | str | NBA season string |
| `games` | int | Home games played |
| `home_wins` | int | Home wins |
| `avg_home_score` | float | Average points scored at home |
| `avg_opponent_score` | float | Average opponent points at home |

#### `gold.v_soccer_league_standings`

Cumulative league table for soccer. One row per team per league per season. Ranked by points, then goal difference, then goals scored.

| Column | Type | Description |
|--------|------|-------------|
| `league` | str | League name |
| `season` | str | Season string |
| `rank` | int | Position in the table |
| `team` | str | Team name |
| `matches_played` | int | Total matches (home + away) |
| `wins` | int | Total wins |
| `draws` | int | Total draws |
| `losses` | int | Total losses |
| `goals_for` | int | Total goals scored |
| `goals_against` | int | Total goals conceded |
| `goal_difference` | int | `goals_for - goals_against` |
| `points` | int | `wins * 3 + draws` |
| `xg_for` | float | Cumulative expected goals for |
| `xg_against` | float | Cumulative expected goals against |
| `xg_difference` | float | `xg_for - xg_against` |

#### `gold.v_nba_standings`

Cumulative NBA standings. One row per team per season. Ranked by win percentage.

| Column | Type | Description |
|--------|------|-------------|
| `season` | str | NBA season string |
| `rank` | int | Position in standings |
| `team` | str | Team name |
| `team_id` | int | NBA team ID |
| `games_played` | int | Total games (home + away) |
| `wins` | int | Total wins |
| `losses` | int | Total losses |
| `win_pct` | float | `wins / games_played` (3 decimal places) |
| `home_wins` | int | Wins at home |
| `home_losses` | int | Losses at home |
| `away_wins` | int | Wins on the road |
| `away_losses` | int | Losses on the road |
| `points_per_game` | float | Average points scored |
| `points_allowed_per_game` | float | Average points allowed |
| `point_differential` | float | Average scoring margin |

#### `gold.v_soccer_home_away_splits`

Home vs away performance breakdown for soccer teams. One row per team per league per season.

| Column | Type | Description |
|--------|------|-------------|
| `team` | str | Team name |
| `league` | str | League name |
| `season` | str | Season string |
| `home_matches` | int | Matches played at home |
| `home_wins` | int | Home wins |
| `home_draws` | int | Home draws |
| `home_losses` | int | Home losses |
| `home_goals_for` | int | Goals scored at home |
| `home_goals_against` | int | Goals conceded at home |
| `home_xg_for` | float | xG at home |
| `home_xg_against` | float | xG against at home |
| `away_matches` | int | Matches played away |
| `away_wins` | int | Away wins |
| `away_draws` | int | Away draws |
| `away_losses` | int | Away losses |
| `away_goals_for` | int | Goals scored away |
| `away_goals_against` | int | Goals conceded away |
| `away_xg_for` | float | xG away |
| `away_xg_against` | float | xG against away |

#### `gold.v_nfl_standings`

Cumulative NFL standings. One row per team per season. Ranked by win percentage, then point differential.

| Column | Type | Description |
|--------|------|-------------|
| `season` | int | NFL season year |
| `rank` | int | Position in standings |
| `team` | str | Team full name |
| `games_played` | int | Total games (home + away) |
| `wins` | int | Total wins |
| `losses` | int | Total losses |
| `ties` | int | Total ties |
| `win_pct` | float | `wins / games_played` (3 decimal places) |
| `home_wins` | int | Wins at home |
| `home_losses` | int | Losses at home |
| `away_wins` | int | Wins on the road |
| `away_losses` | int | Losses on the road |
| `points_for` | int | Total points scored |
| `points_against` | int | Total points allowed |
| `point_differential` | int | `points_for - points_against` |
| `points_per_game` | float | Average points scored |
| `points_allowed_per_game` | float | Average points allowed |

#### `gold.v_nba_home_away_splits`

Home vs away performance breakdown for NBA teams. One row per team per season.

| Column | Type | Description |
|--------|------|-------------|
| `team` | str | Team name |
| `team_id` | int | NBA team ID |
| `season` | str | NBA season string |
| `home_games` | int | Games played at home |
| `home_wins` | int | Home wins |
| `home_losses` | int | Home losses |
| `home_avg_points_scored` | float | Average points at home |
| `home_avg_points_allowed` | float | Average opponent points at home |
| `away_games` | int | Games played on the road |
| `away_wins` | int | Away wins |
| `away_losses` | int | Away losses |
| `away_avg_points_scored` | float | Average points on the road |
| `away_avg_points_allowed` | float | Average opponent points on the road |

---

## Glossary

| Term | Definition |
|------|-----------|
| **xG** (Expected Goals) | Statistical measure of the quality of scoring chances. An xG of 1.0 means the average player would score 1 goal from those chances. |
| **xA** (Expected Assists) | Expected goals from chances created by a player's passes. |
| **PPR Fantasy Points** | Points-per-reception fantasy scoring format. |
| **Net Rating** | Points scored minus points allowed per 100 possessions (NBA). |
| **Pace** | Estimated number of possessions per 48 minutes (NBA). |
| **Spread Line** | Vegas point spread; negative means the home team is favored (NFL). |
| **Total Line** | Vegas over/under for combined game score (NFL). |
| **Form String** | Compact representation of recent results: `W` = win, `D` = draw, `L` = loss. Read left to right, newest to oldest. |
