# Data Audit Report
**Generated:** 2026-02-16
**Data source:** `data/raw/` — 10 CSV files (extracted from zip archives)
**Total rows:** 289,121
**Total columns:** 48

---

## 1. Files Loaded

| File | Rows |
|------|------|
| `racing_data_2015.csv` | 30,395 |
| `racing_data_2016.csv` | 28,897 |
| `racing_data_2017.csv` | 28,439 |
| `racing_data_2018.csv` | 27,647 |
| `racing_data_2019.csv` | 28,352 |
| `racing_data_2020.csv` | 28,086 |
| `racing_data_2021.csv` | 28,688 |
| `racing_data_2022.csv` | 29,569 |
| `racing_data_2023.csv` | 28,690 |
| `racing_data_2024.csv` | 30,358 |

---

## 2. Row Counts by Year

| Year | Rows |
|------|------|
| 2015 | 30,395 |
| 2016 | 28,897 |
| 2017 | 28,439 |
| 2018 | 27,647 |
| 2019 | 28,352 |
| 2020 | 28,086 |
| 2021 | 28,688 |
| 2022 | 29,569 |
| 2023 | 28,690 |
| 2024 | 30,358 |
| **Total** | **289,121** |

---

## 3. Complete Column Inventory

| # | Column | dtype | Total | Missing | % Missing | Unique | Min | Max | Sample Values |
|---|--------|-------|-------|---------|-----------|--------|-----|-----|---------------|
| 1 | `race_id` | str | 289,121 | 0 | 0.0% | 25,529 | - | - | 20200730ROS06, 20170508MUS03, 20190622NEW03, 20181002GOO03 |
| 2 | `race_date` | str | 289,121 | 0 | 0.0% | 2,433 | - | - | 2020-07-30, 2017-05-08, 2019-06-22, 2018-10-02 |
| 3 | `race_time` | str | 289,121 | 0 | 0.0% | 22,598 | - | - | 2020-07-30 13:25:00, 2017-05-08 13:25:00, 2019-06-22 13:15:00, 2018-10-02 17:30:00 |
| 4 | `course` | str | 289,121 | 0 | 0.0% | 76 | - | - | Roscommon, Musselburgh, Newcastle, Goodwood |
| 5 | `course_config` | str | 289,121 | 270,030 | 93.4% | 4 | - | - | Round, Round, Round, Round |
| 6 | `country` | str | 289,121 | 0 | 0.0% | 2 | - | - | IRE, GB, GB, GB |
| 7 | `surface` | str | 289,121 | 0 | 0.0% | 4 | - | - | Turf, Turf, Tapeta, Turf |
| 8 | `race_type` | str | 289,121 | 0 | 0.0% | 20 | - | - | Hurdle Listed, Chase, Flat Maiden, Novice Hurdle |
| 9 | `race_class` | int64 | 289,121 | 0 | 0.0% | 6 | 1 | 6 | 2, 6, 5, 4 |
| 10 | `distance_description` | str | 289,121 | 0 | 0.0% | 23 | - | - | 3m2f, 2m1f, 5f34y, 2m5f |
| 11 | `distance_yards` | int64 | 289,121 | 0 | 0.0% | 23 | 1100 | 7040 | 5720, 3740, 1133, 4620 |
| 12 | `distance_furlongs` | float64 | 289,121 | 0 | 0.0% | 23 | 5.0 | 32.0 | 26.0, 17.0, 5.15, 21.0 |
| 13 | `going_description` | str | 289,121 | 0 | 0.0% | 12 | - | - | Soft, Soft to Heavy, Standard, Soft |
| 14 | `going_stick` | float64 | 289,121 | 115,699 | 40.02% | 91 | 3.0 | 12.0 | 7.7, 3.1, 5.4, 7.4 |
| 15 | `rail_movement` | str | 289,121 | 203,689 | 70.45% | 10 | - | - | Rail moved 40 yards from true, Rail moved 15 yards from true, Rail moved 40 yards from true, Rail moved 25 yards from true |
| 16 | `rail_movement_yards` | float64 | 289,121 | 203,689 | 70.45% | 10 | 5.0 | 50.0 | 40.0, 15.0, 40.0, 25.0 |
| 17 | `wind_speed_mph` | float64 | 289,121 | 85,425 | 29.55% | 301 | 0.0 | 30.0 | 4.1, 25.8, 8.2, 14.3 |
| 18 | `wind_direction` | str | 289,121 | 85,773 | 29.67% | 8 | - | - | SW, NW, NW, S |
| 19 | `field_size` | int64 | 289,121 | 0 | 0.0% | 18 | 3 | 20 | 8, 16, 14, 10 |
| 20 | `horse_id` | int64 | 289,121 | 0 | 0.0% | 288,837 | 201500001 | 202430328 | 202017772, 201712074, 201910552, 201818451 |
| 21 | `horse_name` | str | 289,121 | 0 | 0.0% | 1,332 | - | - | Black Thunder, River Blaze, Diamond Hawk, Magic Hawk |
| 22 | `age` | int64 | 289,121 | 0 | 0.0% | 11 | 0 | 11 | 11, 4, 3, 11 |
| 23 | `sex` | str | 289,121 | 0 | 0.0% | 5 | - | - | C, G, G, M |
| 24 | `weight_st` | int64 | 289,121 | 0 | 0.0% | 5 | 8 | 12 | 10, 10, 9, 11 |
| 25 | `weight_lb` | int64 | 289,121 | 0 | 0.0% | 14 | 0 | 13 | 4, 13, 8, 10 |
| 26 | `weight_lbs` | int64 | 289,121 | 0 | 0.0% | 71 | 112 | 200 | 144, 153, 134, 164 |
| 27 | `jockey_claim_lbs` | float64 | 289,121 | 245,438 | 84.89% | 3 | 3.0 | 7.0 | 3.0, 3.0, 3.0, 7.0 |
| 28 | `draw` | float64 | 289,121 | 162,450 | 56.19% | 20 | 1.0 | 20.0 | 13.0, 1.0, 9.0, 2.0 |
| 29 | `finishing_position` | str | 289,121 | 0 | 0.0% | 25 | - | - | 4, 12, 4, 4 |
| 30 | `beaten_lengths_cumulative` | float64 | 289,121 | 4,242 | 1.47% | 2,624 | 0.0 | 37.25 | 2.66, 1.28, 15.46, 2.56 |
| 31 | `beaten_lengths_description` | str | 289,121 | 4,242 | 1.47% | 2,596 | - | - | 2.66, 1.28, 15.46, 2.56 |
| 32 | `finishing_time_secs` | float64 | 289,121 | 5,083 | 1.76% | 41,296 | -1.0 | 521.28 | 432.57, 431.68, 436.19, 298.46 |
| 33 | `winning_time_secs` | float64 | 289,121 | 857 | 0.3% | 17,727 | 55.51 | 519.06 | 211.12, 234.82, 299.91, 232.88 |
| 34 | `status` | str | 289,121 | 0 | 0.0% | 6 | - | - | Finished, Finished, Finished, Finished |
| 35 | `official_rating` | float64 | 289,121 | 14,603 | 5.05% | 111 | 25.0 | 135.0 | 90.0, 125.0, 110.0, 72.0 |
| 36 | `rpr` | float64 | 289,121 | 11,693 | 4.04% | 127 | 17.0 | 143.0 | 73.0, 92.0, 67.0, 49.0 |
| 37 | `tfig` | float64 | 289,121 | 17,411 | 6.02% | 125 | 16.0 | 140.0 | 81.0, 89.0, 57.0, 48.0 |
| 38 | `jockey` | str | 289,121 | 0 | 0.0% | 45 | - | - | D Jacob, K Brogan, S Levey, N de Boinville |
| 39 | `trainer` | str | 289,121 | 0 | 0.0% | 35 | - | - | A O'Brien, D O'Meara, M Scudamore, D McCain |
| 40 | `equipment` | str | 289,121 | 111,744 | 38.65% | 8 | - | - | t, b, v, e/s |
| 41 | `in_running_position` | str | 289,121 | 115,826 | 40.06% | 1,140 | - | - | 1/2/6, 3/11/12, 9/10/11, 2/4/10 |
| 42 | `sectional_time_last2f` | float64 | 289,121 | 235,783 | 81.55% | 603 | 20.02 | 26.52 | 24.76, 24.95, 25.35, 22.24 |
| 43 | `overweight_lbs` | float64 | 289,121 | 260,122 | 89.97% | 3 | 0.0 | 2.0 | 0.0, 0.0, 0.0, 0.0 |
| 44 | `headgear` | str | 289,121 | 111,744 | 38.65% | 8 | - | - | t, b, v, e/s |
| 45 | `comment` | float64 | 289,121 | 289,121 | 100.0% | 0 | nan | nan |  |
| 46 | `sp_decimal` | float64 | 289,121 | 0 | 0.0% | 9,951 | 1.5 | 101.0 | 1.61, 21.2, 63.02, 77.13 |
| 47 | `sp_fraction` | float64 | 289,121 | 289,121 | 100.0% | 0 | nan | nan |  |

---

## 4. Target Variable: TFig

- **Column name:** `tfig`
- **Description:** Timeform Figure — the target variable for ML training (see framework §17.4)
- **Total values:** 289,121
- **Missing / empty:** 17,411 (6.02%)
- **Valid numeric values:** 271,710
- **Mean:** 81.4
- **Median:** 80.0
- **Std Dev:** 23.4
- **Min:** 16
- **Max:** 140
- **25th percentile:** 63
- **75th percentile:** 99

---

## 5. Column-to-Framework Variable Mapping

Each column mapped to its corresponding speed figure framework variable (from `docs/speed_figure_framework_v2.md`).

| Column | Framework Variable |
|--------|--------------------|
| `race_id` | Race Identifier |
| `race_date` | Race Date |
| `race_time` | Race Time |
| `course` | Course Name |
| `course_config` | Course Configuration |
| `country` | Country (GB/IRE) |
| `surface` | Surface Type (Turf/AW) |
| `race_type` | Race Type (flat/jump/handicap/maiden etc.) |
| `race_class` | Race Class |
| `distance_description` | Distance (text label) |
| `distance_yards` | Distance (yards) |
| `distance_furlongs` | Distance (furlongs) |
| `going_description` | Going Description |
| `going_stick` | Going Stick Reading |
| `rail_movement` | Rail Movement Description |
| `rail_movement_yards` | Rail Movement (yards) |
| `wind_speed_mph` | Wind Speed (mph) |
| `wind_direction` | Wind Direction |
| `field_size` | Field Size |
| `horse_id` | Horse Identifier |
| `horse_name` | Horse Name |
| `age` | Horse Age |
| `sex` | Horse Sex |
| `weight_st` | Weight Carried (stones component) |
| `weight_lb` | Weight Carried (lbs component) |
| `weight_lbs` | Weight Carried (total lbs) |
| `jockey_claim_lbs` | Jockey Claim (lbs) |
| `draw` | Draw / Stall Position |
| `finishing_position` | Finishing Position |
| `beaten_lengths_cumulative` | Beaten Lengths (cumulative) |
| `beaten_lengths_description` | Beaten Lengths (text description) |
| `finishing_time_secs` | Finishing Time (individual runner) |
| `winning_time_secs` | Winning Time (race) |
| `status` | Run Status (Finished/PU/F/UR etc.) |
| `official_rating` | Official Rating (OR) |
| `rpr` | Racing Post Rating (RPR) |
| `tfig` | TFig (Timeform Figure — TARGET VARIABLE) |
| `jockey` | Jockey |
| `trainer` | Trainer |
| `equipment` | Equipment/Headgear Code |
| `in_running_position` | In-Running Position |
| `sectional_time_last2f` | Sectional Timing (last 2f) |
| `overweight_lbs` | Overweight (lbs) |
| `headgear` | Headgear |
| `comment` | Race Comment |
| `sp_decimal` | Starting Price (decimal) |
| `sp_fraction` | Starting Price (fractional) |

### Columns NOT in the framework (potential extra features)

- `_year` — derived column (extracted from `race_date` during audit; not in raw CSVs)

---

## 6. Framework Variables Missing from Data

All **required** framework variables are present in the data.

All **nice-to-have** framework variables are present in the data.

### Framework variables absent entirely (must be sourced or derived):

- Run-up distance
- Temperature / precipitation at race time
- Jockey/Trainer win % (rolling)
- Equipment change flag (first-time blinkers etc.)
- Pace position at call points (1f, half, etc.)
- Finishing speed percentage (Timeform method)
- Trainer/Jockey combo win %
- Horse prior figure history (median last 3/6, best career, trend)
- Days since last run
- Sire / Dam / Breeding data

---

## 7. Unique Courses, Distances, and Surfaces

### Courses (76 unique)

**GB (51):** Aintree, Ascot, Ayr, Bath, Beverley, Brighton, Carlisle, Catterick, Chelmsford, Cheltenham, Chepstow, Chester, Doncaster, Epsom, Exeter, Ffos Las, Fontwell, Goodwood, Hamilton, Haydock, Hexham, Huntingdon, Kempton, Leicester, Lingfield, Ludlow, Market Rasen, Musselburgh, Newbury, Newcastle, Newmarket, Nottingham, Plumpton, Pontefract, Redcar, Ripon, Salisbury, Sandown, Sedgefield, Southwell, Stratford, Taunton, Thirsk, Uttoxeter, Warwick, Wetherby, Wincanton, Windsor, Wolverhampton, Worcester, York

**IRE (25):** Ballinrobe, Bellewstown, Clonmel, Cork, Curragh, Down Royal, Downpatrick, Dundalk, Fairyhouse, Galway, Gowran Park, Kilbeggan, Killarney, Leopardstown, Limerick, Listowel, Naas, Navan, Punchestown, Roscommon, Sligo, Thurles, Tipperary, Tramore, Wexford

### Distances (23 unique values in furlongs)

5.0, 5.15, 6.0, 6.07, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 24.0, 25.0, 26.0, 29.0, 32.0

### Surface Types (4 unique)

- **Fibresand**: 3,957 rows (1.4%)
- **Polytrack**: 15,761 rows (5.5%)
- **Tapeta**: 7,068 rows (2.4%)
- **Turf**: 262,335 rows (90.7%)

---

## 8. Data Quality Issues

### Duplicates
- **Full row duplicates:** 283
- **Key duplicates (race_id + horse_id):** 284

### Impossible / Suspicious Values

- finishing_time_secs: 570 negative values
- age: 283 rows with age=0 (impossible)
- weight_lbs: 4060 rows with weight>180lbs (impossible for flat, suspicious for jump)

### Missing Critical Fields

| Column | Missing Count | % Missing | Impact |
|--------|---------------|-----------|--------|
| `winning_time_secs` | 857 | 0.3% | Cannot compute speed figure for these rows |
| `finishing_time_secs` | 5,083 | 1.76% | Cannot compute speed figure for these rows |
| `beaten_lengths_cumulative` | 4,242 | 1.47% | Cannot compute placed-horse figures |
| `weight_lbs` | 0 | 0.0% | Cannot apply weight adjustment |
| `going_description` | 0 | 0.0% | Cannot determine going allowance context |
| `distance_furlongs` | 0 | 0.0% | Data gap |
| `course` | 0 | 0.0% | Data gap |
| `race_class` | 0 | 0.0% | Data gap |
| `age` | 0 | 0.0% | Data gap |
| `sex` | 0 | 0.0% | Data gap |
| `finishing_position` | 0 | 0.0% | Data gap |
| `tfig` | 17,411 | 6.02% | TARGET — missing rows cannot be used for ML training |

---

## 9. Going Description Distribution

| Going | Count | % |
|-------|-------|---|
| Soft to Heavy | 34,512 | 11.9% |
| Hard | 34,289 | 11.9% |
| Soft | 34,256 | 11.8% |
| Heavy | 32,857 | 11.4% |
| Good | 32,794 | 11.3% |
| Firm | 32,149 | 11.1% |
| Good to Soft | 31,637 | 10.9% |
| Good to Firm | 29,841 | 10.3% |
| Standard to Fast | 8,542 | 3.0% |
| Standard | 6,849 | 2.4% |
| Slow | 6,539 | 2.3% |
| Standard to Slow | 4,856 | 1.7% |

---

## 10. Race Type Distribution

| Race Type | Count | % |
|-----------|-------|---|
| Flat Conditions | 15,782 | 5.5% |
| Flat Novice | 15,616 | 5.4% |
| Flat | 15,208 | 5.3% |
| Flat Handicap | 14,893 | 5.2% |
| Flat Group 3 | 14,779 | 5.1% |
| Novice Hurdle | 14,571 | 5.0% |
| Chase Listed | 14,551 | 5.0% |
| Flat Maiden | 14,551 | 5.0% |
| Chase | 14,383 | 5.0% |
| Hurdle Listed | 14,372 | 5.0% |
| Flat Group 2 | 14,354 | 5.0% |
| Flat Group 1 | 14,208 | 4.9% |
| Hurdle | 14,182 | 4.9% |
| Flat Listed | 14,167 | 4.9% |
| Novice Chase | 14,124 | 4.9% |
| Bumper | 13,979 | 4.8% |
| Hurdle Handicap | 13,976 | 4.8% |
| Hurdle Graded | 13,946 | 4.8% |
| Chase Handicap | 13,879 | 4.8% |
| Chase Graded | 13,600 | 4.7% |

---

## 11. Race Class Distribution

| Class | Count | % |
|-------|-------|---|
| 1 | 48,755 | 16.9% |
| 2 | 48,536 | 16.8% |
| 3 | 47,860 | 16.6% |
| 4 | 47,258 | 16.3% |
| 5 | 48,025 | 16.6% |
| 6 | 48,687 | 16.8% |

---

## 12. Sex Distribution

| Code | Sex | Count | % |
|------|-----|-------|---|
| M | Mare | 58,014 | 20.1% |
| H | Horse (entire) | 57,956 | 20.0% |
| C | Colt | 57,844 | 20.0% |
| F | Filly | 57,679 | 19.9% |
| G | Gelding | 57,628 | 19.9% |

---

## 13. Age Distribution

| Age | Count | % |
|-----|-------|---|
| 0 | 283 | 0.1% |
| 2 | 26,676 | 9.2% |
| 3 | 26,616 | 9.2% |
| 4 | 46,393 | 16.0% |
| 5 | 46,303 | 16.0% |
| 6 | 45,987 | 15.9% |
| 7 | 19,411 | 6.7% |
| 8 | 19,432 | 6.7% |
| 9 | 19,341 | 6.7% |
| 10 | 19,329 | 6.7% |
| 11 | 19,350 | 6.7% |

---

## 14. Sectional Timing Coverage

- **Rows with sectional timing data:** 53,338 (18.4%)
- **Rows without:** 235,783 (81.6%)
- Sectional data is available primarily for flat races at equipped courses.
- Mean last-2f time (where available): 23.14s
- This maps to the framework's 'Finishing Speed %' methodology (§9.2)

---

## 15. Wind & Weather Coverage

- **Rows with wind speed data:** 203,696 (70.5%)
- **Mean wind speed:** 14.9 mph
- **Max wind speed:** 30.0 mph

- **Rows with wind direction:** 203,348
- **Directions observed:** E, N, NE, NW, S, SE, SW, W

---

## 16. Rail Movement Coverage

- **Rows with rail movement data:** 85,432 (29.5%)
- **Mean movement (where present):** 21.7 yards
- **Max movement:** 50 yards

---

## 17. Summary & Recommendations

### Strengths
- 10 years of data (2015-2024) with ~289K runner records
- All **critical** framework variables are present
- TFig (target variable) available for ~94% of rows — sufficient for ML training
- Sectional timing available for ~22% of rows (flat races at equipped courses)
- Wind and rail movement data present at meeting level
- Both GB and IRE courses covered
- All surface types represented (Turf, Polytrack, Tapeta, Fibresand)

### Issues to Address Before Building Speed Figures
1. **Remove duplicate rows** — ~0.1% of data are exact duplicates
2. **Fix impossible values** — negative finishing times, age=0, weight>180lbs
3. **Handle missing winning times** — ~0.3% of rows have no timing data
4. **Standardise finishing_position** — contains both numeric positions and status codes (PU, F, UR)
5. **Validate beaten_lengths_cumulative** — ensure cumulative calculation is consistent
6. **Source additional data** — run-up distances, temperature/precipitation, breeding info
7. **Derive rolling features** — jockey/trainer win %, horse prior figures, days since run

### Next Steps (per framework roadmap §23)
1. Build `src/column_mapping.py` to standardise column names across pipeline
2. Clean data: remove duplicates, fix impossible values, handle missing fields
3. Build standard times per course/distance/configuration (§2)
4. Build lbs-per-length tables (§3)
5. Implement going allowance calculation (§4)
6. Produce first raw speed figures and compare to TFig
