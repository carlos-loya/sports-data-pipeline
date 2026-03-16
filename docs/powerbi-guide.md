# Importing Silver Data into Power BI

This guide explains how to load the pipeline's silver-layer Parquet files into Power BI Desktop.

## Available Datasets

After running the backfill, the following silver Parquet files are generated:

| File | Description | Key columns |
|------|-------------|-------------|
| `data/silver/basketball/games.parquet` | NBA game results | game_id, game_date, home_team, away_team, home_score, away_score, home_win |
| `data/silver/basketball/player_stats.parquet` | NBA player game logs | player_name, team, game_date, points, rebounds, assists, steals, blocks |
| `data/silver/football/games.parquet` | NFL game results | game_id, season, week, home_team, away_team, home_score, away_score, home_win |
| `data/silver/football/player_stats.parquet` | NFL player weekly stats | player_name, team, position, passing_yards, rushing_yards, receiving_yards, fantasy_points |
| `data/silver/soccer/matches.parquet` | Soccer match results | match_id, league, match_date, home_team, away_team, home_goals, away_goals, result |

## Step 1: Locate the Files

If you are running Power BI on Windows with the pipeline in WSL, the files are accessible at:

```
\\wsl.localhost\<distro-name>\home\<user>\<repo-path>\data\silver\
```

For example:

```
\\wsl.localhost\Ubuntu-24.04\home\loya\src\github.com\carlos-loya\sports-data-pipeline\data\silver\
```

You can find your distro name by running `wsl -l` in a Windows terminal.

## Step 2: Import into Power BI

1. Open **Power BI Desktop**
2. Click **Get Data** > **Parquet**
3. Paste the **full file path** to the `.parquet` file (not the directory). For example:

   ```
   \\wsl.localhost\Ubuntu-24.04\home\loya\src\github.com\carlos-loya\sports-data-pipeline\data\silver\basketball\games.parquet
   ```

4. Click **OK**, then **Load** (or **Transform Data** to preview first)
5. Repeat for each dataset you want to import

> **Important:** Point Power BI to the individual `.parquet` file, not the folder. Power BI may duplicate the path if you point to a directory.

## Step 3: Build Relationships

After importing multiple tables, set up relationships in the **Model** view:

- **NBA**: Link `games.game_id` to `player_stats.game_id`
- **NFL**: Link `games.game_id` and `games.week` to `player_stats` (join on season + week for player context)

## Troubleshooting

### "Could not find a part of the path"

Power BI is appending the folder path twice. Make sure you are pointing to the **file**, not the directory:
- Correct: `\\wsl.localhost\...\basketball\games.parquet`
- Wrong: `\\wsl.localhost\...\basketball\`

### "'dataType' argument cannot be null"

This means a column in the Parquet file has all null values and Power BI cannot infer its type. Re-run the backfill pipeline to regenerate the file — this usually indicates a bug in the extractor that has since been fixed.

### WSL path not accessible

Make sure WSL is running. You can verify by opening a Windows terminal and running:

```
wsl -l --running
```

### Alternative: Copy to Windows

If WSL paths don't work reliably, copy the files to a Windows-native path:

```bash
# From inside WSL
cp data/silver/basketball/games.parquet /mnt/c/Users/<your-username>/Documents/
cp data/silver/basketball/player_stats.parquet /mnt/c/Users/<your-username>/Documents/
```

Then import from `C:\Users\<your-username>\Documents\` in Power BI.

## Refreshing Data

After running a new backfill, the silver Parquet files are overwritten with the latest data. In Power BI:

1. Click **Refresh** in the Home ribbon to reload all datasets
2. Or right-click a specific table in the Fields pane > **Refresh data**
