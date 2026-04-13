# Column Dictionary

This file documents the standardized Parquet column names produced by `football_data_to_gcs.py`.

The source data comes from `football-data.co.uk`. During ingestion, column names are normalized to make them safe for BigQuery and easier to query consistently across seasons.

The stored schema is also standardized:

- identifier and descriptive columns are written as strings
- count-like match measures such as goals, shots, corners, fouls, cards, and bookmaker counts are written as integers
- odds and handicap-style values are written as floating-point numeric values

## Naming Rules

- All column names are converted to lowercase.
- Spaces and punctuation are converted to underscores.
- Dots are removed by converting them to underscores.
- Common operators are converted to readable tokens:
  - `>` -> `_gt_`
  - `<` -> `_lt_`
  - `>=` -> `_gte_`
  - `<=` -> `_lte_`
  - `=` -> `_eq_`
  - `%` -> `_pct_`
- Repeated underscores are collapsed to a single underscore.
- If a normalized name starts with a digit, it is prefixed with `col_`.
- If two source columns normalize to the same target name, a numeric suffix is added, for example `_2`.

Examples:

- `HomeTeam` -> `hometeam`
- `B365>2.5` -> `b365_gt_2_5`
- `B365<2.5` -> `b365_lt_2_5`
- `B365C>2.5` -> `b365c_gt_2_5`

## Core Match Columns

| Normalized column | Original column | Meaning |
| --- | --- | --- |
| `div` | `Div` | League division code from the source file. |
| `country` | `Country` | Country name in combined extra-league files. |
| `league` | `League` | League name in combined extra-league files. |
| `season` | `Season` | Season identifier in combined extra-league files. |
| `source_url` | generated | Original Football-Data CSV URL used to create the Parquet file. |
| `source_type` | generated | Ingestion mode: `seasonal` for standard country pages, `combined` for extra-league combined CSVs. |
| `date` | `Date` | Match date. |
| `time` | `Time` | Kickoff time when present. |
| `hometeam` | `HomeTeam` | Home team name. |
| `awayteam` | `AwayTeam` | Away team name. |
| `fthg` | `FTHG` or `HG` | Full-time home goals. |
| `ftag` | `FTAG` or `AG` | Full-time away goals. |
| `ftr` | `FTR` or `Res` | Full-time result: `H` home win, `D` draw, `A` away win. |
| `hthg` | `HTHG` | Half-time home goals. |
| `htag` | `HTAG` | Half-time away goals. |
| `htr` | `HTR` | Half-time result: `H`, `D`, `A`. |
| `attendance` | `Attendance` | Match attendance when present. |
| `referee` | `Referee` | Match referee. |

## Match Statistics Columns

| Normalized column | Original column | Meaning |
| --- | --- | --- |
| `hs` | `HS` | Home team shots. |
| `as` | `AS` | Away team shots. |
| `hst` | `HST` | Home team shots on target. |
| `ast` | `AST` | Away team shots on target. |
| `hc` | `HC` | Home team corners. |
| `ac` | `AC` | Away team corners. |
| `hf` | `HF` | Home team fouls committed. |
| `af` | `AF` | Away team fouls committed. |
| `hy` | `HY` | Home team yellow cards. |
| `ay` | `AY` | Away team yellow cards. |
| `hr` | `HR` | Home team red cards. |
| `ar` | `AR` | Away team red cards. |

## Match Odds: Home / Draw / Away

These columns represent standard 1X2 match odds:

- `h` = home win
- `d` = draw
- `a` = away win

Examples:

| Normalized column | Original column | Meaning |
| --- | --- | --- |
| `b365h` | `B365H` | Bet365 home win odds. |
| `b365d` | `B365D` | Bet365 draw odds. |
| `b365a` | `B365A` | Bet365 away win odds. |
| `psh` | `PSH` | Pinnacle home win odds. |
| `psd` | `PSD` | Pinnacle draw odds. |
| `psa` | `PSA` | Pinnacle away win odds. |
| `whh` | `WHH` | William Hill home win odds. |
| `whd` | `WHD` | William Hill draw odds. |
| `wha` | `WHA` | William Hill away win odds. |
| `vch` | `VCH` | VC Bet home win odds. |
| `vcd` | `VCD` | VC Bet draw odds. |
| `vca` | `VCA` | VC Bet away win odds. |
| `maxh` | `MaxH` | Market maximum home win odds. |
| `maxd` | `MaxD` | Market maximum draw odds. |
| `maxa` | `MaxA` | Market maximum away win odds. |
| `avgh` | `AvgH` | Market average home win odds. |
| `avgd` | `AvgD` | Market average draw odds. |
| `avga` | `AvgA` | Market average away win odds. |

## Closing Match Odds

Columns with `c` before the final outcome suffix represent closing odds.

Examples:

| Normalized column | Original column | Meaning |
| --- | --- | --- |
| `b365ch` | `B365CH` | Bet365 closing home win odds. |
| `b365cd` | `B365CD` | Bet365 closing draw odds. |
| `b365ca` | `B365CA` | Bet365 closing away win odds. |
| `psch` | `PSCH` | Pinnacle closing home win odds. |
| `pscd` | `PSCD` | Pinnacle closing draw odds. |
| `psca` | `PSCA` | Pinnacle closing away win odds. |
| `maxch` | `MaxCH` | Market maximum closing home win odds. |
| `maxcd` | `MaxCD` | Market maximum closing draw odds. |
| `maxca` | `MaxCA` | Market maximum closing away win odds. |
| `avgch` | `AvgCH` | Market average closing home win odds. |
| `avgcd` | `AvgCD` | Market average closing draw odds. |
| `avgca` | `AvgCA` | Market average closing away win odds. |

## Over / Under 2.5 Goals Odds

These columns describe the over/under 2.5 goals market:

- `gt_2_5` = over 2.5 goals
- `lt_2_5` = under 2.5 goals

Examples:

| Normalized column | Original column | Meaning |
| --- | --- | --- |
| `b365_gt_2_5` | `B365>2.5` | Bet365 over 2.5 goals odds. |
| `b365_lt_2_5` | `B365<2.5` | Bet365 under 2.5 goals odds. |
| `p_gt_2_5` | `P>2.5` | Pinnacle over 2.5 goals odds. |
| `p_lt_2_5` | `P<2.5` | Pinnacle under 2.5 goals odds. |
| `max_gt_2_5` | `Max>2.5` | Market maximum over 2.5 goals odds. |
| `max_lt_2_5` | `Max<2.5` | Market maximum under 2.5 goals odds. |
| `avg_gt_2_5` | `Avg>2.5` | Market average over 2.5 goals odds. |
| `avg_lt_2_5` | `Avg<2.5` | Market average under 2.5 goals odds. |
| `b365c_gt_2_5` | `B365C>2.5` | Bet365 closing over 2.5 goals odds. |
| `b365c_lt_2_5` | `B365C<2.5` | Bet365 closing under 2.5 goals odds. |
| `pc_gt_2_5` | `PC>2.5` | Pinnacle closing over 2.5 goals odds. |
| `pc_lt_2_5` | `PC<2.5` | Pinnacle closing under 2.5 goals odds. |

## Asian Handicap Odds

These columns relate to Asian handicap betting:

- `ahh` = Asian handicap home price
- `aha` = Asian handicap away price
- `ahh` as a standalone source column may also represent the handicap line value itself depending on season
- `ahch` typically represents the closing handicap line value

Examples:

| Normalized column | Original column | Meaning |
| --- | --- | --- |
| `ahh` | `AHh` | Asian handicap line, usually quoted from the home team perspective. |
| `b365ahh` | `B365AHH` | Bet365 Asian handicap home odds. |
| `b365aha` | `B365AHA` | Bet365 Asian handicap away odds. |
| `pahh` | `PAHH` | Pinnacle Asian handicap home odds. |
| `paha` | `PAHA` | Pinnacle Asian handicap away odds. |
| `maxahh` | `MaxAHH` | Market maximum Asian handicap home odds. |
| `maxaha` | `MaxAHA` | Market maximum Asian handicap away odds. |
| `avgahh` | `AvgAHH` | Market average Asian handicap home odds. |
| `avgaha` | `AvgAHA` | Market average Asian handicap away odds. |
| `ahch` | `AHCh` | Closing Asian handicap line. |
| `b365cahh` | `B365CAHH` | Bet365 closing Asian handicap home odds. |
| `b365caha` | `B365CAHA` | Bet365 closing Asian handicap away odds. |
| `pcahh` | `PCAHH` | Pinnacle closing Asian handicap home odds. |
| `pcaha` | `PCAHA` | Pinnacle closing Asian handicap away odds. |

## Bookmaker / Market Prefixes

These prefixes commonly appear in football-data files:

| Prefix | Meaning |
| --- | --- |
| `b365` | Bet365 |
| `bw` | Bet&Win |
| `gb` | Gamebookers |
| `iw` | Interwetten |
| `lb` | Ladbrokes |
| `ps` | Pinnacle Sports |
| `wh` | William Hill |
| `vc` | VC Bet |
| `sj` | Stan James |
| `bs` | Blue Square |

## Market Aggregate Columns

These columns summarize bookmaker groups rather than one bookmaker.

| Normalized column | Original column | Meaning |
| --- | --- | --- |
| `bb1x2` | `Bb1X2` | Number of bookmakers used to calculate 1X2 market averages and maximums. |
| `bbmxh` | `BbMxH` | Best available home win odds across tracked bookmakers. |
| `bbmxd` | `BbMxD` | Best available draw odds across tracked bookmakers. |
| `bbmxa` | `BbMxA` | Best available away win odds across tracked bookmakers. |
| `bbavh` | `BbAvH` | Average home win odds across tracked bookmakers. |
| `bbavd` | `BbAvD` | Average draw odds across tracked bookmakers. |
| `bbava` | `BbAvA` | Average away win odds across tracked bookmakers. |
| `bbou` | `BbOU` | Number of bookmakers used for over/under averages and maximums. |
| `bbmx_gt_2_5` | `BbMx>2.5` | Best available over 2.5 goals odds across tracked bookmakers. |
| `bbmx_lt_2_5` | `BbMx<2.5` | Best available under 2.5 goals odds across tracked bookmakers. |
| `bbav_gt_2_5` | `BbAv>2.5` | Average over 2.5 goals odds across tracked bookmakers. |
| `bbav_lt_2_5` | `BbAv<2.5` | Average under 2.5 goals odds across tracked bookmakers. |
| `bbah` | `BbAH` | Number of bookmakers used for Asian handicap averages and maximums. |
| `bbmxahh` | `BbMxAHH` | Best available Asian handicap home odds across tracked bookmakers. |
| `bbmxaha` | `BbMxAHA` | Best available Asian handicap away odds across tracked bookmakers. |
| `bbavahh` | `BbAvAHH` | Average Asian handicap home odds across tracked bookmakers. |
| `bbavaha` | `BbAvAHA` | Average Asian handicap away odds across tracked bookmakers. |

## Notes

- Not every season contains every column.
- Older seasons contain fewer statistics and fewer odds markets.
- Extra leagues often use combined files with explicit `country`, `league`, and `season` columns before the ingestion process partitions them into `country/league/season` paths.
- Some very old files may contain additional columns not listed here. Those will still be normalized using the same naming rules.

## Sources

- Football-Data notes and data pages:
  - https://www.football-data.co.uk/data.php
  - https://www.football-data.co.uk/downloadm.php
  - https://www.football-data.co.uk/all_new_data.php
