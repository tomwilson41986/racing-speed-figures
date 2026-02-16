"""
Column Mapping Configuration
=============================
Maps raw CSV column names to standardised framework variable names.
Import this module in all subsequent pipeline scripts to ensure consistency.

Usage:
    from src.column_mapping import RAW_TO_STD, STD_TO_RAW, COLUMNS

    # Rename a DataFrame from raw to standard names:
    df = df.rename(columns=RAW_TO_STD)

    # Access a specific standard column name:
    time_col = COLUMNS['winning_time']
"""

# ---------------------------------------------------------------------------
# RAW_TO_STD: maps raw CSV column names → standardised framework names
# ---------------------------------------------------------------------------
RAW_TO_STD = {
    # Identifiers
    'race_id':                      'race_id',
    'race_date':                    'race_date',
    'race_time':                    'race_datetime',
    'horse_id':                     'horse_id',
    'horse_name':                   'horse_name',

    # Course / venue
    'course':                       'course_name',
    'course_config':                'course_config',
    'country':                      'country',

    # Race descriptors
    'race_type':                    'race_type',
    'race_class':                   'race_class',
    'surface':                      'surface_type',
    'field_size':                   'field_size',

    # Distance
    'distance_description':         'distance_desc',
    'distance_yards':               'distance_yards',
    'distance_furlongs':            'distance_furlongs',

    # Going / ground
    'going_description':            'going_desc',
    'going_stick':                  'going_stick_reading',

    # Timing
    'winning_time_secs':            'winning_time',
    'finishing_time_secs':          'finishing_time',
    'sectional_time_last2f':        'sectional_last_2f',

    # Result
    'finishing_position':           'finish_pos',
    'beaten_lengths_cumulative':    'beaten_lengths',
    'beaten_lengths_description':   'beaten_lengths_desc',
    'status':                       'run_status',

    # Horse attributes
    'age':                          'horse_age',
    'sex':                          'horse_sex',

    # Weight
    'weight_st':                    'weight_st',
    'weight_lb':                    'weight_lb',
    'weight_lbs':                   'weight_carried_lbs',
    'jockey_claim_lbs':             'jockey_claim_lbs',
    'overweight_lbs':               'overweight_lbs',

    # Draw
    'draw':                         'draw',

    # Connections
    'jockey':                       'jockey',
    'trainer':                      'trainer',

    # Equipment
    'equipment':                    'equipment',
    'headgear':                     'headgear',

    # Environmental
    'wind_speed_mph':               'wind_speed',
    'wind_direction':               'wind_direction',
    'rail_movement':                'rail_movement_desc',
    'rail_movement_yards':          'rail_movement_yards',

    # In-running
    'in_running_position':          'in_running_pos',

    # Ratings (existing)
    'official_rating':              'official_rating',
    'rpr':                          'rpr',
    'tfig':                         'tfig',

    # Market
    'sp_decimal':                   'sp_decimal',
    'sp_fraction':                  'sp_fraction',

    # Other
    'comment':                      'comment',
}

# ---------------------------------------------------------------------------
# STD_TO_RAW: reverse mapping (standard name → raw CSV column name)
# ---------------------------------------------------------------------------
STD_TO_RAW = {v: k for k, v in RAW_TO_STD.items()}

# ---------------------------------------------------------------------------
# COLUMNS: quick-access dict for standard column names by semantic role
# ---------------------------------------------------------------------------
COLUMNS = {
    # --- Identifiers ---
    'race_id':              'race_id',
    'race_date':            'race_date',
    'race_datetime':        'race_datetime',
    'horse_id':             'horse_id',
    'horse_name':           'horse_name',

    # --- Course ---
    'course_name':          'course_name',
    'course_config':        'course_config',
    'country':              'country',

    # --- Race ---
    'race_type':            'race_type',
    'race_class':           'race_class',
    'surface_type':         'surface_type',
    'field_size':           'field_size',

    # --- Distance ---
    'distance_desc':        'distance_desc',
    'distance_yards':       'distance_yards',
    'distance_furlongs':    'distance_furlongs',

    # --- Going ---
    'going_desc':           'going_desc',
    'going_stick_reading':  'going_stick_reading',

    # --- Timing ---
    'winning_time':         'winning_time',
    'finishing_time':        'finishing_time',
    'sectional_last_2f':    'sectional_last_2f',

    # --- Result ---
    'finish_pos':           'finish_pos',
    'beaten_lengths':       'beaten_lengths',
    'beaten_lengths_desc':  'beaten_lengths_desc',
    'run_status':           'run_status',

    # --- Horse ---
    'horse_age':            'horse_age',
    'horse_sex':            'horse_sex',

    # --- Weight ---
    'weight_carried_lbs':   'weight_carried_lbs',
    'weight_st':            'weight_st',
    'weight_lb':            'weight_lb',
    'jockey_claim_lbs':     'jockey_claim_lbs',
    'overweight_lbs':       'overweight_lbs',

    # --- Draw ---
    'draw':                 'draw',

    # --- Connections ---
    'jockey':               'jockey',
    'trainer':              'trainer',

    # --- Equipment ---
    'equipment':            'equipment',
    'headgear':             'headgear',

    # --- Environmental ---
    'wind_speed':           'wind_speed',
    'wind_direction':       'wind_direction',
    'rail_movement_desc':   'rail_movement_desc',
    'rail_movement_yards':  'rail_movement_yards',

    # --- In-Running ---
    'in_running_pos':       'in_running_pos',

    # --- Ratings ---
    'official_rating':      'official_rating',
    'rpr':                  'rpr',
    'tfig':                 'tfig',          # TARGET variable for ML training

    # --- Market ---
    'sp_decimal':           'sp_decimal',
    'sp_fraction':          'sp_fraction',

    # --- Other ---
    'comment':              'comment',
}

# ---------------------------------------------------------------------------
# TARGET: the column we train against
# ---------------------------------------------------------------------------
TARGET = 'tfig'

# ---------------------------------------------------------------------------
# Key constants from the framework (§2.3)
# ---------------------------------------------------------------------------
CONSTANTS = {
    'seconds_per_length':       0.2,
    'lengths_per_second':       5.0,
    'lbs_per_second_at_5f':     22.0,
    'furlongs_per_mile':        8.0,
    'yards_per_furlong':        220.0,
    'base_weight_flat_lbs':     126,    # 9st 0lb
    'base_weight_jump_lbs':     154,    # 11st 0lb
    'base_rating':              100,
}

# ---------------------------------------------------------------------------
# Going description → numeric ordinal (for models and sorting)
# Higher = firmer ground; lower = softer ground
# ---------------------------------------------------------------------------
GOING_ORDER = {
    'Hard':             8,
    'Firm':             7,
    'Good to Firm':     6,
    'Good':             5,
    'Good to Soft':     4,
    'Soft':             3,
    'Soft to Heavy':    2,
    'Heavy':            1,
    # AW surfaces
    'Standard to Fast': 7,
    'Standard':         5,
    'Standard to Slow': 3,
    'Slow':             2,
}

# ---------------------------------------------------------------------------
# Sex code → description
# ---------------------------------------------------------------------------
SEX_LABELS = {
    'C': 'Colt',
    'G': 'Gelding',
    'F': 'Filly',
    'M': 'Mare',
    'H': 'Horse (entire)',
}

# ---------------------------------------------------------------------------
# Race type classification helpers
# ---------------------------------------------------------------------------
JUMP_RACE_TYPES = {
    'Hurdle', 'Hurdle Handicap', 'Chase', 'Chase Handicap',
    'Novice Hurdle', 'Novice Chase', 'Bumper',
    'Hurdle Listed', 'Hurdle Graded', 'Chase Listed', 'Chase Graded',
}

FLAT_RACE_TYPES = {
    'Flat', 'Flat Handicap', 'Flat Maiden', 'Flat Novice',
    'Flat Conditions', 'Flat Listed', 'Flat Group 3',
    'Flat Group 2', 'Flat Group 1',
}

AW_COURSES = {
    'Chelmsford', 'Kempton', 'Lingfield',
    'Newcastle', 'Wolverhampton', 'Southwell',
    'Dundalk',
}


def is_jump_race(race_type: str) -> bool:
    """Return True if the race type is a jump (NH) race."""
    return race_type in JUMP_RACE_TYPES


def is_flat_race(race_type: str) -> bool:
    """Return True if the race type is a flat race."""
    return race_type in FLAT_RACE_TYPES


def is_aw_course(course_name: str) -> bool:
    """Return True if the course is an all-weather venue."""
    return course_name in AW_COURSES
