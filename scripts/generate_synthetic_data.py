"""
Generate synthetic UK/Ireland racing data for development and testing.
Produces CSV files resembling real racing data provider exports with realistic
column names, distributions, and data quality issues.
"""

import csv
import random
import os
import zipfile
from datetime import datetime, timedelta

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')

# --- Reference data ---

UK_COURSES = [
    'Ascot', 'Aintree', 'Ayr', 'Bath', 'Beverley', 'Brighton', 'Carlisle',
    'Catterick', 'Chelmsford', 'Cheltenham', 'Chepstow', 'Chester',
    'Doncaster', 'Epsom', 'Exeter', 'Ffos Las', 'Fontwell', 'Goodwood',
    'Hamilton', 'Haydock', 'Hexham', 'Huntingdon', 'Kempton', 'Leicester',
    'Lingfield', 'Ludlow', 'Market Rasen', 'Musselburgh', 'Newbury',
    'Newcastle', 'Newmarket', 'Nottingham', 'Plumpton', 'Pontefract',
    'Redcar', 'Ripon', 'Salisbury', 'Sandown', 'Sedgefield', 'Southwell',
    'Stratford', 'Taunton', 'Thirsk', 'Uttoxeter', 'Warwick', 'Wetherby',
    'Wincanton', 'Windsor', 'Wolverhampton', 'Worcester', 'York',
]

IRE_COURSES = [
    'Leopardstown', 'Curragh', 'Fairyhouse', 'Punchestown', 'Galway',
    'Cork', 'Limerick', 'Navan', 'Naas', 'Tipperary', 'Dundalk',
    'Gowran Park', 'Killarney', 'Listowel', 'Wexford', 'Down Royal',
    'Downpatrick', 'Ballinrobe', 'Clonmel', 'Tramore', 'Sligo',
    'Roscommon', 'Kilbeggan', 'Thurles', 'Bellewstown',
]

ALL_COURSES = UK_COURSES + IRE_COURSES

AW_COURSES = {
    'Chelmsford': 'Polytrack', 'Kempton': 'Polytrack', 'Lingfield': 'Polytrack',
    'Newcastle': 'Tapeta', 'Wolverhampton': 'Tapeta', 'Southwell': 'Fibresand',
    'Dundalk': 'Polytrack',
}

GOING_DESCRIPTIONS = [
    'Hard', 'Firm', 'Good to Firm', 'Good', 'Good to Soft', 'Soft',
    'Soft to Heavy', 'Heavy',
]

AW_GOING = ['Standard', 'Standard to Fast', 'Standard to Slow', 'Slow']

FLAT_DISTANCES_F = [5.0, 5.15, 6.0, 6.07, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 16.0]
JUMP_DISTANCES_F = [16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 24.0, 25.0, 26.0, 29.0, 32.0]

FLAT_DISTANCE_LABELS = {
    5.0: '5f', 5.15: '5f34y', 6.0: '6f', 6.07: '6f16y', 7.0: '7f',
    8.0: '1m', 9.0: '1m1f', 10.0: '1m2f', 11.0: '1m3f', 12.0: '1m4f',
    13.0: '1m5f', 14.0: '1m6f', 16.0: '2m',
}

JUMP_DISTANCE_LABELS = {
    16.0: '2m', 17.0: '2m1f', 18.0: '2m2f', 19.0: '2m3f', 20.0: '2m4f',
    21.0: '2m5f', 24.0: '3m', 25.0: '3m1f', 26.0: '3m2f', 29.0: '3m5f',
    32.0: '4m',
}

RACE_TYPES_FLAT = ['Flat', 'Flat Handicap', 'Flat Maiden', 'Flat Novice',
                   'Flat Conditions', 'Flat Listed', 'Flat Group 3',
                   'Flat Group 2', 'Flat Group 1']
RACE_TYPES_JUMP = ['Hurdle', 'Hurdle Handicap', 'Chase', 'Chase Handicap',
                   'Novice Hurdle', 'Novice Chase', 'Bumper',
                   'Hurdle Listed', 'Hurdle Graded', 'Chase Listed', 'Chase Graded']

RACE_CLASSES = [1, 2, 3, 4, 5, 6, 7]  # 7 = lowest

SEX_CODES = ['C', 'G', 'F', 'M', 'H']  # colt, gelding, filly, mare, horse (entire)

JOCKEYS_FLAT = [
    'R Moore', 'W Buick', 'T Marquand', 'J Doyle', 'O Murphy',
    'R Havlin', 'D Tudhope', 'J Spencer', 'A Kirby', 'H Bentley',
    'L Dettori', 'S De Sousa', 'C Soumillon', 'D Egan', 'C Lee',
    'B Curtis', 'R Kingscote', 'K Shoemark', 'Hector Crouch', 'J Mitchell',
    'P Mulrennan', 'S Levey', 'L Morris', 'C Bishop', 'T Eaves',
]

JOCKEYS_JUMP = [
    'H Cobden', 'N de Boinville', 'Rachael Blackmore', 'P Townend',
    'S Twiston-Davies', 'H Skelton', 'A Coleman', 'B Powell', 'Tom Cannon',
    'J McGrath', 'S Bowen', 'B Geraghty', 'D Jacob', 'A Heskin',
    'A Wedge', 'K Brogan', 'C Gethings', 'T Scudamore', 'J Quinlan',
    'B Cooper',
]

TRAINERS = [
    'A Balding', 'J Gosden', 'C Appleby', 'A O\'Brien', 'W Haggas',
    'R Varian', 'S bin Suroor', 'M Johnston', 'K Ryan', 'R Hannon',
    'D O\'Meara', 'H Palmer', 'C Cox', 'T Clover', 'E Walker',
    'W Mullins', 'G Elliott', 'N Henderson', 'P Nicholls', 'D Skelton',
    'O Sherwood', 'K Bailey', 'A King', 'J O\'Neill', 'E Lavelle',
    'H de Bromhead', 'J Harrington', 'E O\'Grady', 'T Mullins',
    'N Twiston-Davies', 'C Longsdon', 'B Pauling', 'D Pipe',
    'M Scudamore', 'D McCain',
]

EQUIPMENT = ['', '', '', '', '', 'b', 'v', 't', 'p', 'h', 'b/v', 'e/s', 'h/t']

# Standard time approximations (seconds) per furlong at good going, for a median-class runner
BASE_SPF = {
    5.0: 12.0, 5.15: 12.0, 6.0: 12.1, 6.07: 12.1, 7.0: 12.2,
    8.0: 12.35, 9.0: 12.5, 10.0: 12.65, 11.0: 12.8, 12.0: 12.95,
    13.0: 13.1, 14.0: 13.2, 16.0: 13.4,
    17.0: 13.5, 18.0: 13.6, 19.0: 13.7, 20.0: 13.8, 21.0: 13.85,
    24.0: 14.1, 25.0: 14.2, 26.0: 14.3, 29.0: 14.5, 32.0: 14.7,
}


def rand_date(year):
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def generate_race_id(date, course, race_num):
    return f"{date.strftime('%Y%m%d')}{course[:3].upper()}{race_num:02d}"


def generate_winning_time(distance_f, going, is_jump):
    spf = BASE_SPF.get(distance_f, 13.0)
    # going adjustment
    going_adj = {
        'Hard': -0.35, 'Firm': -0.2, 'Good to Firm': -0.08, 'Good': 0.0,
        'Good to Soft': 0.15, 'Soft': 0.4, 'Soft to Heavy': 0.7, 'Heavy': 1.1,
        'Standard': 0.0, 'Standard to Fast': -0.1, 'Standard to Slow': 0.15, 'Slow': 0.3,
    }
    adj = going_adj.get(going, 0.0)
    base = spf * distance_f
    if is_jump:
        # jumps are slower — add fence/hurdle time
        base += distance_f * 0.3
    time = base + (adj * distance_f) + random.gauss(0, 1.5)
    return round(max(time, distance_f * 9), 2)


def generate_beaten_length(pos, field_size):
    if pos == 1:
        return 0.0
    # cumulative beaten lengths grows with position
    bl = 0.0
    for p in range(2, pos + 1):
        gap = random.expovariate(1.5) + 0.05
        if p > field_size * 0.7:
            gap *= 2  # tail runners beaten further
        bl += gap
    return round(bl, 2)


def beaten_length_str(bl):
    if bl == 0:
        return '0'
    if bl <= 0.1:
        return 'shd'
    if bl <= 0.2:
        return 'hd'
    if bl <= 0.35:
        return 'nk'
    return str(bl)


def generate_or(race_class, age, is_jump):
    base = {1: 110, 2: 95, 3: 82, 4: 70, 5: 60, 6: 50, 7: 40}
    b = base.get(race_class, 65)
    if is_jump:
        b += 10
    or_val = b + random.randint(-15, 15)
    if age == 2:
        or_val -= 10
    return max(0, or_val)


def generate_rpr(or_val):
    return max(0, or_val + random.randint(-8, 8))


def generate_tfig(or_val, going, weight_lbs, age, distance_f):
    """Generate TFig: a Timeform-style figure correlated to OR but with noise."""
    base = or_val + random.randint(-5, 5)
    # slight going effect
    if going in ('Heavy', 'Soft to Heavy'):
        base -= random.randint(0, 4)
    # weight penalty
    if weight_lbs > 133:
        base -= random.randint(0, 2)
    # age bonus for young improvers
    if age <= 3:
        base += random.randint(0, 3)
    return max(0, base)


def generate_horse_name():
    prefixes = ['Royal', 'Golden', 'Silver', 'Black', 'Red', 'Blue', 'Dark',
                'Swift', 'Noble', 'Mighty', 'Grand', 'Wild', 'Iron', 'Flying',
                'Spirit', 'Lucky', 'Storm', 'Thunder', 'Rock', 'Star', 'Fire',
                'Silk', 'Crystal', 'Diamond', 'Shadow', 'Magic', 'Power',
                'King', 'Queen', 'Prince', 'Lady', 'Lord', 'Sea', 'Desert',
                'City', 'Mountain', 'River']
    suffixes = ['Arrow', 'Dancer', 'Runner', 'Knight', 'Dream', 'Quest',
                'Spirit', 'Storm', 'Thunder', 'Fire', 'Star', 'Light',
                'King', 'Prince', 'Queen', 'Lady', 'Heart', 'Song',
                'Hope', 'Glory', 'Pride', 'Moon', 'Sun', 'Wind',
                'Blaze', 'Charm', 'Flash', 'Hawk', 'Eagle', 'Rose',
                'Grace', 'Legend', 'Force', 'Gem', 'Belle', 'Duke']
    return f"{random.choice(prefixes)} {random.choice(suffixes)}"


def generate_one_year(year, n_meetings=None):
    """Generate one year of racing data."""
    if n_meetings is None:
        n_meetings = random.randint(380, 420)  # ~400 meetings/year UK+IRE

    rows = []
    horse_id_counter = year * 100000

    for meeting_idx in range(n_meetings):
        date = rand_date(year)
        course = random.choice(ALL_COURSES)
        is_aw = course in AW_COURSES
        surface = AW_COURSES.get(course, 'Turf')

        # Determine going
        if is_aw:
            going = random.choice(AW_GOING)
        else:
            going = random.choice(GOING_DESCRIPTIONS)

        # Going stick reading (sometimes missing)
        going_stick = round(random.uniform(3.0, 12.0), 1) if random.random() < 0.6 else None

        # Number of races per meeting
        n_races = random.randint(5, 8)

        # Rail movements (sometimes)
        rail_movement = ''
        rail_movement_yards = None
        if random.random() < 0.3:
            yards = random.choice([5, 8, 10, 12, 15, 20, 25, 30, 40, 50])
            rail_movement = f"Rail moved {yards} yards from true"
            rail_movement_yards = yards

        # Wind
        wind_speed = round(random.uniform(0, 30), 1) if random.random() < 0.7 else None
        wind_dir_options = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        wind_dir = random.choice(wind_dir_options) if wind_speed else None

        for race_num in range(1, n_races + 1):
            race_id = generate_race_id(date, course, race_num)

            # Flat or jump
            is_jump = random.random() < 0.45  # ~45% jump racing
            if course in ['Cheltenham', 'Aintree', 'Punchestown', 'Fairyhouse',
                          'Exeter', 'Ludlow', 'Plumpton', 'Sedgefield',
                          'Wetherby', 'Wincanton', 'Hexham', 'Huntingdon',
                          'Fontwell', 'Market Rasen', 'Uttoxeter', 'Stratford',
                          'Taunton', 'Warwick', 'Worcester']:
                is_jump = True
            if is_aw:
                is_jump = False

            if is_jump:
                distance_f = random.choice(JUMP_DISTANCES_F)
                dist_label = JUMP_DISTANCE_LABELS.get(distance_f, f'{distance_f}f')
                race_type = random.choice(RACE_TYPES_JUMP)
            else:
                distance_f = random.choice(FLAT_DISTANCES_F)
                dist_label = FLAT_DISTANCE_LABELS.get(distance_f, f'{distance_f}f')
                race_type = random.choice(RACE_TYPES_FLAT)

            distance_yards = int(distance_f * 220)
            race_class = random.choice(RACE_CLASSES[:6])  # 1-6

            # Field size
            field_size = random.randint(3, 20)
            if is_jump:
                field_size = min(field_size, 16)

            # Winning time
            win_time = generate_winning_time(distance_f, going, is_jump)

            # Race time
            race_time = f"{date.strftime('%Y-%m-%d')} {random.randint(13, 18)}:{random.choice(['00', '15', '25', '30', '35', '40', '45', '50'])}:00"

            # Generate runners
            for pos in range(1, field_size + 1):
                horse_id_counter += 1
                horse_name = generate_horse_name()

                sex = random.choice(SEX_CODES)
                age = random.randint(2, 6) if not is_jump else random.randint(4, 11)
                if age == 2 and is_jump:
                    age = 4

                # Weight
                if is_jump:
                    weight_st = random.randint(10, 12)
                    weight_lb = random.randint(0, 13)
                else:
                    weight_st = random.randint(8, 10)
                    weight_lb = random.randint(0, 13)
                weight_lbs_total = weight_st * 14 + weight_lb

                # Draw (flat only)
                draw = pos if not is_jump and random.random() < 0.95 else None
                if draw and draw > field_size:
                    draw = random.randint(1, field_size)

                # Beaten lengths
                beaten_lengths = generate_beaten_length(pos, field_size)

                # Finishing time
                time_behind = beaten_lengths * 0.2
                fin_time = round(win_time + time_behind, 2) if pos <= field_size else None

                # DNF / pulled up / fell etc
                status = 'Finished'
                if is_jump and pos > field_size * 0.75 and random.random() < 0.1:
                    status = random.choice(['PU', 'F', 'UR', 'BD', 'RO'])
                    fin_time = None
                    beaten_lengths = None

                # OR, RPR, TFig
                or_val = generate_or(race_class, age, is_jump)
                rpr = generate_rpr(or_val)
                tfig = generate_tfig(or_val, going, weight_lbs_total, age, distance_f)

                # Sometimes OR/RPR/TFig missing for first-time runners
                if random.random() < 0.05:
                    or_val = None
                if random.random() < 0.04:
                    rpr = None
                if random.random() < 0.06:
                    tfig = None

                # Jockey
                if is_jump:
                    jockey = random.choice(JOCKEYS_JUMP)
                else:
                    jockey = random.choice(JOCKEYS_FLAT)

                trainer = random.choice(TRAINERS)
                equip = random.choice(EQUIPMENT)

                # Claiming jockey
                claim_lbs = 0
                if random.random() < 0.15:
                    claim_lbs = random.choice([3, 5, 7])

                # Sectional time (last 2f) — available ~40% of flat races
                sectional_last2f = None
                if not is_jump and random.random() < 0.4:
                    spf = BASE_SPF.get(distance_f, 12.5)
                    sectional_last2f = round(2 * (spf - random.uniform(0.5, 1.5)) + random.gauss(0, 0.3), 2)

                # In-running position
                irp = ''
                if random.random() < 0.6:
                    irp_positions = sorted(random.sample(range(1, field_size + 1), min(3, field_size)))
                    irp = '/'.join(str(p) for p in irp_positions)

                # Course configuration
                config = ''
                if course == 'Newmarket':
                    config = random.choice(['Rowley Mile', 'July Course'])
                elif course == 'Ascot':
                    config = random.choice(['Round', 'Straight']) if distance_f <= 8 else 'Round'
                elif course in ('Newbury', 'York', 'Doncaster'):
                    config = random.choice(['Round', 'Straight']) if distance_f <= 7 else 'Round'

                row = {
                    'race_id': race_id,
                    'race_date': date.strftime('%Y-%m-%d'),
                    'race_time': race_time,
                    'course': course,
                    'course_config': config,
                    'country': 'IRE' if course in IRE_COURSES else 'GB',
                    'surface': surface,
                    'race_type': race_type,
                    'race_class': race_class,
                    'distance_description': dist_label,
                    'distance_yards': distance_yards,
                    'distance_furlongs': distance_f,
                    'going_description': going,
                    'going_stick': going_stick,
                    'rail_movement': rail_movement,
                    'rail_movement_yards': rail_movement_yards,
                    'wind_speed_mph': wind_speed,
                    'wind_direction': wind_dir,
                    'field_size': field_size,
                    'horse_id': horse_id_counter,
                    'horse_name': horse_name,
                    'age': age,
                    'sex': sex,
                    'weight_st': weight_st,
                    'weight_lb': weight_lb,
                    'weight_lbs': weight_lbs_total,
                    'jockey_claim_lbs': claim_lbs if claim_lbs > 0 else '',
                    'draw': draw if draw is not None else '',
                    'finishing_position': pos if status == 'Finished' else status,
                    'beaten_lengths_cumulative': beaten_lengths if beaten_lengths is not None else '',
                    'beaten_lengths_description': beaten_length_str(beaten_lengths) if beaten_lengths is not None else '',
                    'finishing_time_secs': fin_time if fin_time else '',
                    'winning_time_secs': win_time,
                    'status': status,
                    'official_rating': or_val if or_val is not None else '',
                    'rpr': rpr if rpr is not None else '',
                    'tfig': tfig if tfig is not None else '',
                    'jockey': jockey,
                    'trainer': trainer,
                    'equipment': equip,
                    'in_running_position': irp,
                    'sectional_time_last2f': sectional_last2f if sectional_last2f is not None else '',
                    'overweight_lbs': random.choice([0, 0, 0, 0, 0, 0, 0, 1, 2]) if random.random() < 0.1 else '',
                    'headgear': equip if equip else '',
                    'comment': '',
                    'sp_decimal': round(random.uniform(1.5, 101.0), 2),
                    'sp_fraction': '',  # left empty for simplicity
                }

                rows.append(row)

    return rows


def introduce_data_quality_issues(rows):
    """Introduce realistic data quality issues."""
    n = len(rows)

    # 1. Some duplicate rows (~0.1%)
    n_dups = int(n * 0.001)
    for _ in range(n_dups):
        idx = random.randint(0, n - 1)
        rows.append(dict(rows[idx]))

    # 2. Some impossible values
    for _ in range(int(n * 0.002)):
        idx = random.randint(0, n - 1)
        rows[idx]['finishing_time_secs'] = -1.0  # impossible negative time

    for _ in range(int(n * 0.001)):
        idx = random.randint(0, n - 1)
        rows[idx]['age'] = 0  # impossible age

    for _ in range(int(n * 0.001)):
        idx = random.randint(0, n - 1)
        rows[idx]['weight_lbs'] = 200  # impossibly heavy

    # 3. Some missing winning times
    for _ in range(int(n * 0.003)):
        idx = random.randint(0, n - 1)
        rows[idx]['winning_time_secs'] = ''
        rows[idx]['finishing_time_secs'] = ''

    return rows


def write_csv(filename, rows):
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return filepath


def create_zip(csv_path, zip_name):
    zip_path = os.path.join(OUTPUT_DIR, zip_name)
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, os.path.basename(csv_path))
    os.remove(csv_path)
    return zip_path


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_rows = []
    years = range(2015, 2025)  # 10 years: 2015-2024

    for year in years:
        print(f"Generating data for {year}...")
        rows = generate_one_year(year)
        rows = introduce_data_quality_issues(rows)
        random.shuffle(rows)
        all_rows.extend(rows)

        csv_name = f'racing_data_{year}.csv'
        csv_path = write_csv(csv_name, rows)
        zip_name = f'racing_data_{year}.zip'
        create_zip(csv_path, zip_name)
        print(f"  -> {zip_name} ({len(rows)} rows)")

    print(f"\nTotal rows across all years: {len(all_rows)}")
    print("All zip files created in data/raw/")


if __name__ == '__main__':
    main()
