# ============================================================
# IMPORTS
# ============================================================
import pandas as pd
import requests
import io
import os
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from pathlib import Path
# from twilio.rest import Client
from dotenv import load_dotenv
from datetime import date

# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv("ev_dashboard.env")

DB_URL = os.getenv("DATABASE_URL")

# ============================================================
# INITIALIZE CLIENTS
# ============================================================
try:
    engine = create_engine(DB_URL)
    print("Database engine successfully created.")
except:
    print("Error: No DB Engine detected")

def run_pipeline():
    # ============================================================
    # STEP 1: IMPORT CHARGER DATA URL
    # ============================================================

    # pull raw chargers data 
    print("Step 1: Checking for latest chargers data on GOV.UK...")
    # base_url = 'https://www.gov.uk/government/statistical-data-sets/electric-vehicle-charging-infrastructure-statistics-data-tables-evci'
    # reqs = requests.get(base_url)
    # soup = BeautifulSoup(reqs.text, 'html.parser')

    # find link to relevant gov data
    # links = [link.get('href') for link in soup.find_all('a') if link.get('href')]
    # data_page = next(l for l in links if ('https://www.gov.uk/government/statistics/electric-vehicle-public-charging-infrastructure-statistics') 
    #                  in l)

    # pull EV charger dowload link
    base_url = 'https://www.gov.uk/government/statistical-data-sets/electric-vehicle-charging-infrastructure-statistics-data-tables-evci'
    ch_req = requests.get(base_url)
    ch_soup = BeautifulSoup(ch_req.text, 'html.parser')

    ch_url = next(link.get('href') for link in ch_soup.find_all('a')
                         if 'evci9001_' in link.get('href'))
    
    file_id = ch_url.split('/')[-2]

    # ============================================================
    # STEP 2: MEMORY CHECK
    # ============================================================
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE IF NOT EXISTS processed_files ( \
                           filename TEXT PRIMARY KEY, \
                           processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
                           )"""))
        exists = conn.execute(text("SELECT 1 FROM processed_files WHERE filename = :f"), 
                              {"f": file_id}).fetchone()

    if exists:
        print(f"Skipping: Data release {file_id} has already been processed.")
        return 

    # ============================================================
    # STEP 3: CLEAN CHARGERS DATA
    # ============================================================

    # ============================================================
    #    STEP 3.1: CLEAN TOTAL CHARGERS DATA
    # ============================================================
    print("Step 2: New data found. Updating...")
    print("Step 2: Updating total chargers...")
    ch_content = requests.get(ch_url,stream=True).content

    ch_data = pd.read_excel(io.BytesIO(ch_content), engine='odf', sheet_name=None)

    # ==== clean main chargers data ====
    # remove top row note
    ch = ch_data['1a'].iloc[1:]

    # set top row as col titles
    ch.columns = ch.iloc[0]

    # rename cols
    ch = (ch.iloc[1:]
            .rename(columns={
                'Local authority / region code': 'region_ons', 
                'Local authority / region name': 'region_name'
            }))
    
    # pivot to long
    ch = ch.melt(id_vars=['region_ons', 'region_name'], var_name='quarter', 
                 value_name='all_chargers')

    # remove non-numeric values like '[x]'
    ch['all_chargers'] = pd.to_numeric(ch['all_chargers'], errors='coerce')

    # remove notes in quarter/region_name cols
    for col in ['region_name', 'quarter']:
        ch[col] = ch[col].str.replace(r'\s*\[Note \d+\]$', '', regex=True).str.strip()

    # convert quarter to date
    ch['quarter'] = pd.to_datetime(ch['quarter'], format='%b-%y').dt.date

    # ==== clean fast chargers data ====
    # remove top row note
    ch_fast = ch_data['1b'].iloc[1:]

    # set top row as col titles
    ch_fast.columns = ch_fast.iloc[0]

    # rename cols
    ch_fast = (ch_fast.iloc[1:]
               .rename(columns={
                'Local authority / region code': 'region_ons', 
                'Local authority / region name': 'region_name'
               }))

    # pivot to long
    ch_fast = ch_fast.melt(id_vars=['region_ons', 'region_name'], 
                           var_name='quarter', value_name='fast_chargers')

    # remove non-numeric values like '[x]'
    ch_fast['fast_chargers'] = pd.to_numeric(ch_fast['fast_chargers'], 
                                             errors='coerce')

    #REMEMBER - OCT23 ONWARDS, SPEED DEFINITION CHANGES
    # remove notes in quarter/region_name cols
    for col in ['region_name', 'quarter']:
        ch_fast[col] = ch_fast[col].str.replace(r'\s*\[Note \d+\]$', '', 
                                                regex=True).str.strip()

    # remove speed labels on quarter text
    ch_fast['quarter'] = ch_fast['quarter'].str.replace(r'\(.*?\)', '', 
                                                        regex=True).str.strip()

    # fix quarters with incorrect date formats
    ch_fast['quarter'] = ch_fast['quarter'].apply(
        lambda x: x[0:3] + x[-3:] if len(x) > 6 else x
    )

    # convert quarter to date
    ch_fast['quarter'] = pd.to_datetime(ch_fast['quarter'], format='%b-%y').dt.date

    # correct suspected reporting error in Portsmouth, April 2023
    ch_fast.loc[
        (ch_fast['quarter'].astype(str).str.contains('2023-04-01')) &
        (ch_fast['region_name'].str.contains('Portsmouth')),
        ['fast_chargers']
    ] = [7]

    # ==== join main and fast chargers ====
    # merge chargers main and fast
    ch_joined = ch.merge(ch_fast, on=['region_ons', 'quarter'], how='left')

    # ---- data integrity check against the join ----

        # mismatches = ch_joined[ch_joined['region_name_x'] != ch_joined['region_name_y']]

        # mismatches.head()

    # remame and select columns
    ch_joined = ch_joined.rename(columns={'region_name_x': 'region_name'})[['region_ons', 'region_name', 'quarter', 'all_chargers', 'fast_chargers']]

    # calculate non-fast chargers as 'other chargers'
    ch_joined = ch_joined.assign(other_chargers=ch_joined['all_chargers'] - ch_joined['fast_chargers']) 

    # ============================================================
    #    STEP 3.2: CLEAN CHARGERS PER 100k DATA
    # ============================================================
    print("Step 2: Updating chargers per 100k...")
    
    # remove top row note
    ch_100k = ch_data['2a'].iloc[1:]

    # set top row as col titles
    ch_100k.columns = ch_100k.iloc[0]

    # rename cols
    ch_100k = (ch_100k.iloc[1:]
            .rename(columns={
                'Local authority / region code [Note 5]': 'region_ons',
                'Local authority / region name' : 'region_name'
            }))

    # pivot to long
    ch_100k = ch_100k.melt(id_vars=['region_ons', 'region_name'], var_name='quarter',
                        value_name='all_chargers')

    # remove non-numeric values like '[x]'
    ch_100k['all_chargers'] = pd.to_numeric(ch_100k['all_chargers'], errors='coerce').round(decimals=2)

    # remove notes in quarter/region_name cols
    for col in ['region_name', 'quarter']:
        ch_100k[col] = ch_100k[col].str.replace(r'\s*\[Note \d+\]$', '', regex=True).str.strip()

    # convert quarter to date
    ch_100k['quarter'] = pd.to_datetime(ch_100k['quarter'], format='%b-%y').dt.date

    # ==== clean fast chargers per 100k data ====
    # remove top row note
    ch_fast_100k = ch_data['2b'].iloc[1:]

    # set top row as col titles
    ch_fast_100k.columns = ch_fast_100k.iloc[0]

    # rename cols
    ch_fast_100k = (ch_fast_100k.iloc[1:]
                .rename(columns={
                'Local authority / region code': 'region_ons', 
                'Local authority / region name': 'region_name'
                }))

    # pivot to long
    ch_fast_100k = ch_fast_100k.melt(id_vars=['region_ons', 'region_name'], 
                            var_name='quarter', value_name='fast_chargers')

    # remove non-numeric values like '[x]'
    ch_fast_100k['fast_chargers'] = pd.to_numeric(ch_fast_100k['fast_chargers'], 
                                                errors='coerce').round(decimals=2)

    #REMEMBER - OCT23 ONWARDS, SPEED DEFINITION CHANGES
    # remove notes in quarter/region_name cols
    for col in ['region_name', 'quarter']:
        ch_fast_100k[col] = ch_fast_100k[col].str.replace(r'\s*\[Note \d+\]$', '', 
                                                regex=True).str.strip()

    # remove speed labels on quarter text
    ch_fast_100k['quarter'] = ch_fast_100k['quarter'].str.replace(r'\(.*?\)', '', 
                                                        regex=True).str.strip()

    # fix quarters with incorrect date formats
    ch_fast_100k['quarter'] = ch_fast_100k['quarter'].apply(
        lambda x: x[0:3] + x[-3:] if len(x) > 6 else x
    )

    # convert quarter to date
    ch_fast_100k['quarter'] = pd.to_datetime(ch_fast_100k['quarter'], format='%b-%y').dt.date

    # ==== join main and fast chargers per 100k ====
    # join all chargers and fast chargers
    ch_joined_100k = ch_100k.merge(ch_fast_100k, on=['region_ons', 'quarter'], how='left')

    # rename repeat cols and select final cols
    ch_joined_100k = ch_joined_100k.rename(columns={'region_name_x': 'region_name'})[['region_ons', 'region_name', 'quarter', 'all_chargers', 'fast_chargers']]

    # calculate other chargers column
    ch_joined_100k = ch_joined_100k.assign(other_chargers=ch_joined_100k['all_chargers'] - ch_joined_100k['fast_chargers']).round(decimals=2)

    # ============================================================
    #    STEP 3.3: CLEAN DEVICE SPEEDS DATA
    # ============================================================
    print("Step 2: Updating device speeds...")
    # remove top row notes
    device_speeds = ch_data['4'].iloc[2:]

    # set top row as col titles
    device_speeds.columns = device_speeds.iloc[0]

    # select relevant data
    device_speeds = device_speeds.iloc[1:,:6]

    # create dedicated date column
    device_speeds['date'] = device_speeds['Month'] + " " + device_speeds['Year [Note 1]'].astype(str)

    # place new date col first and select remainder in order
    cols = [-1] + list(range(device_speeds.shape[1] - 1))[2:]

    device_speeds = device_speeds.iloc[7:, cols]

    # rename columns
    device_speeds = device_speeds.rename(
        columns={
            device_speeds.columns[1] : 'Slow',
            device_speeds.columns[2] : 'Fast',
            device_speeds.columns[3] : 'Rapid',
            device_speeds.columns[4] : 'Ultra rapid',
        }
    )

    # drop notes from date col and convert to date
    device_speeds['date'] = device_speeds['date'].str.replace(r'\[.*?\]', '', regex=True).str.strip()

    device_speeds['date'] = pd.to_datetime(device_speeds['date'], format='%B %Y').dt.date

    # pivot to long
    device_speeds = device_speeds.melt(
                        id_vars='date',
                        var_name='speed',
                        value_name='chargers'
                    )

    # convert to numeric 
    device_speeds['chargers'] = pd.to_numeric(device_speeds['chargers'], errors='coerce')

    # calculate categories as proportion of total
    device_speeds['total'] = device_speeds.groupby('date')['chargers'].transform('sum')

    device_speeds['proportion'] = (device_speeds['chargers'] / device_speeds['total']).round(3)

    device_speeds = device_speeds.drop(['total'], axis=1)

    # ============================================================
    #    STEP 3.4: CLEAN DEVICE LOCATION DATA
    # ============================================================
    print("Step 2: Updating device locations...")
    # remove top row notes
    device_location = ch_data['5'].iloc[2:]

    # set top row as col titles
    device_location.columns = device_location.iloc[0]

    # rename cols
    device_location = (device_location.iloc[1:].rename(
            columns={
            device_location.columns[0] : 'year',
            device_location.columns[2] : 'on street',
            device_location.columns[3] : 'destination',
            device_location.columns[4] : 'en-route',
            device_location.columns[5] : 'other',
            device_location.columns[6] : 'total'
            }
    ))

    # drop notes from date col and convert to date
    device_location['Month'] = device_location['Month'].str.replace(r'\[.*?\]', '', regex=True).str.strip()

    device_location['date'] = device_location['Month'].astype(str) + " " + device_location['year'].astype(str)

    device_location['date'] = pd.to_datetime(device_location['date'], format='%B %Y').dt.date

    # place new date col first and select remainder in order
    cols = [-1] + list(range(device_location.shape[1] -1))[2:-1]

    device_location = device_location.iloc[:, cols]

    # pivot to longer
    device_location = device_location.melt(
        id_vars='date',
        var_name='location',
        value_name='chargers'
    )

    # convert to numeric 
    device_location['chargers'] = pd.to_numeric(device_location['chargers'], errors='coerce')

    # calculate categories as proportion of total
    device_location['total'] = device_location.groupby('date')['chargers'].transform('sum')

    device_location['proportion'] = (device_location['chargers'] / device_location['total']).round(3)

    device_location = device_location.drop(['total'], axis=1)

    # ============================================================
    #    STEP 3.5: CLEAN URBAN V RURAL DATA
    # ============================================================
    print("Step 2: Updating urban v rural...")
    # remove top row notes
    urban_rural = ch_data['6a'].iloc[2:]

    # 2. Convert each item: if it's a date, format it; if it's text, strip it
    urban_rural.columns = [
        val.strftime('%b-%y') if hasattr(val, 'strftime') else str(val).strip() 
        for val in urban_rural.iloc[0]
    ]

    # 3. Drop the now-redundant first row from the data
    urban_rural = urban_rural.iloc[1:].reset_index(drop=True)

    urban_rural.drop(urban_rural.tail(1).index,inplace=True)

    # urban_rural = urban_rural.iloc[1:]

    # clean column names
    urban_rural.columns = (
        urban_rural.columns
        .str.replace(r'\[.*?\]', '', regex=True)
        .str.strip()
    )

    # remove notes
    urban_rural.loc[:,'Rural urban classification'] = (
        urban_rural['Rural urban classification']
        .str.replace(r'\[.*?\]', '', regex=True)
        .str.strip()
    )

    # # Focus on main class categories and remove totals
    # filtered_urban_rural = ['Total charging devices in rural areas', 'Total charging devices in urban areas', 'Unknown classification']

    # urban_rural = urban_rural[urban_rural['Rural urban classification'].isin (filtered_urban_rural)].copy()

    # # rename col
    urban_rural = urban_rural.rename(
        columns={'Rural urban classification': 'area_type'}
    )

    # pivot to long
    urban_rural = urban_rural.melt(
        id_vars='area_type',
        var_name='date',
        value_name='chargers'
    )

    # convert to numeric  
    urban_rural['chargers'] = pd.to_numeric(urban_rural['chargers'], errors='coerce')

    # reorder col
    urban_rural = urban_rural.iloc[:,[1, 0, 2]]

    # remove note
    urban_rural['date'] = urban_rural['date'].str.replace(r'\[.*?\]', '', regex=True).str.strip()

    # conver tot date
    urban_rural['date'] = pd.to_datetime(urban_rural['date'], format="%b-%y").dt.date

    # calculate categories as proportion of total
    urban_rural['total'] = urban_rural.groupby('date')['chargers'].transform('sum')

    urban_rural['proportion'] = (urban_rural['chargers'] / urban_rural['total']).round(3)

    urban_rural = urban_rural.drop('total', axis=1)

    # rename categories
    urban_rural.loc[urban_rural['area_type'] == 'Total charging devices in rural areas', 'area_type'] = 'Rural'
    urban_rural.loc[urban_rural['area_type'] == 'Total charging devices in urban areas', 'area_type'] = 'Urban'
    urban_rural.loc[urban_rural['area_type'] == 'Unknown classification', 'area_type'] = 'Unknown'

    # ============================================================
    # STEP 4: IMPORT EV DATA
    # ============================================================
    # pull raw EV data 
    print("Step 3: Updating EV data from GOV.UK...")
    vh_url = 'https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-tables'
    vh_reqs = requests.get(vh_url)
    vh_soup = BeautifulSoup(vh_reqs.text, 'html.parser')

    # find plug-ins ods file
    vh_links = [link.get('href') for link in vh_soup.find_all('a') if link.get('href')]
    vh_data_page = next(l for l in vh_links if ('veh0142.ods') in l)

    # read ods file into a dataframe
    vh_content = pd.read_excel(io.BytesIO(requests.get(vh_data_page,stream=True).content), engine='odf', sheet_name=None)

    # remove top row notes
    evs = vh_content['VEH0142'].iloc[3:]

    # set col names
    evs.columns = evs.iloc[0]
    
    # remove cols from top row
    evs = evs.iloc[1:]

    # remove values with unknown ONS locations
    evs = evs[(evs['ONS Code'] != '[z]')]

    # remove totals category
    evs = evs[(evs['Fuel'] != 'Total')]
    evs = evs[(evs['Keepership'] != 'Total')]
    evs = evs[(evs['BodyType'] != 'Total')]

    # pivot to long
    evs = evs.melt(
        id_vars=evs.iloc[:,:7],
        var_name='date',
        value_name='vehicles'
    )

    # drop cols
    evs.drop(['Units', 'ONS Sort', 'ONS Geography'], axis=1, inplace=True)

    # combine dates to single date col
    evs['date'] = evs['date'].str.replace(' ', '-')

    evs['date'] = pd.PeriodIndex(evs['date'], freq='Q').to_timestamp().date

    # fix capitalization
    cols = ['Fuel', 'Keepership']

    for col in cols:
        evs[col] = evs[col].str.capitalize()

    # convert vehicles to numeric
    evs['vehicles'] = pd.to_numeric(evs['vehicles'], errors='coerce')

    # calculate totals by fuel type
    evs = evs.groupby(['ONS Code', 'date', 'Fuel'])['vehicles'].sum().reset_index()

    evs = evs.groupby(['ONS Code', 'date', 'Fuel'])['vehicles'].sum().reset_index()

    evs = evs.loc[evs['date'] >= date(2019, 10, 1)]

    evs = evs.rename(
        columns={
            'ONS Code': 'region_ons',
            'date': 'quarter',
            'Fuel': 'fuel_type'
            }
    )

    # ============================================================
    # 45. DATABASE UPLOAD & DBT TRIGGER
    # ============================================================
    print("Step 4: Uploading raw data to Supabase...")
    ch_joined.to_sql('chargers', engine, if_exists='replace', index=False)
    ch_joined_100k.to_sql('chargers_per_100k', engine, if_exists='replace', index=False)
    device_speeds.to_sql('devices_speeds', engine, if_exists='replace', index=False)
    device_location.to_sql('devices_locations', engine, if_exists='replace', index=False)
    urban_rural.to_sql('urban_rural', engine, if_exists='replace', index=False)
    evs.to_sql('ev_registrations', engine, if_exists='replace', index=False)

    # check for region lookup
    if Path('region_lookup.csv').exists():
        pd.read_csv('region_lookup.csv').to_sql('region_lookup', engine, if_exists='replace', index=False)

    print("Step 4: Triggering dbt transformations...")
    dbt_result = os.system("dbt run --project-dir ev_transform --profiles-dir ~/.dbt")

    if dbt_result == 0:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO processed_files (filename) VALUES (:f)"), {"f": file_id})
            print(f"Data update successful. Release {file_id} processed. Models and dashboard updated.")
    else:
        print("Error: dbt run failed. Check logs.")

# run function if script run directly
if __name__ == '__main__':
    run_pipeline()