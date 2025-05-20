import pandas as pd
import numpy as np
from faker import Faker
import random
import string
from datetime import datetime

faker = Faker()
Faker.seed(0)
random.seed(0)
np.random.seed(0)

N = 10000  # rows per main table, fewer for dependent tables
def random_lat(): return round(random.uniform(24.8, 25.1), 6)
def random_lon(): return round(random.uniform(66.9, 67.3), 6)

station_ids = []
station_names_lhr = ['City Bus Station', 'Niazi Bus Stand', 'Road Master Bus Terminal', 'Faisal Movers Abdullah Terminal', 'General Bus Stand', 'Jinnah Bus Terminal', 'Canal Metrobus Station', 'Daewoo Express Thokar', 'Azadi Chowk Metro Bus Station', 'Thokar Bus Terminal', 'Skyways Daewoo Bus Terminal', 'Lahore Bus Terminal', 'Qainchi Bus Station', 'Model Town Bus Station']
station_names_khi = ['Bus Station', 'Karachi Bus Adda', 'Daewoo Bus Terminal', 'Punjab Adda', 'Sohrab Goth Bus Stop', 'Faisal Movers Bus Terminal', 'Al Asif New Bus Terminal', 'Saddar Main Bus Stop', 'Faisal Movers', 'Karachi Bus Terminal', 'Banaras Bus Stop', 'Waraich Bus Terminal', 'Green Line Bus Abdullah Chowk Station', 'Taj Complex Bus Stop']
station_names_isl = ['Islamabad Bus Terminal', 'Karachi Company Bus Stop', 'Skyways Bus Terminal', 'Parade Ground Metrobus Station', 'Kashmir Highway Station', 'Faizabad Bus Station', 'PIMS Station', 'N-5 Metro Station', 'Mandi Morr Bus Stand', 'Stock Exchange Station', 'Ibn e Sina Terminal']
station_names_que = ['Sadabahar Bus Station', 'Quetta Taftan Bus Service', 'Al-Mustafa Coach Bus Terminal', 'Jabal e Noor Bus Stop', 'Karachi Coach Terminal', 'Alrauf Bus Terminal', 'Al-Saif Daewoo', 'Moosa Colony Wagon Stop', 'Al Yousaf Bus Service', 'Sohrab Goth Bus Terminal', 'Jabal e Noor Bus Stop']
station_names_psh = ['General Bus Stand', 'Lahore Adda Bus Terminal', 'Niazi Express', 'Daewoo Bus Terminal', 'Faisal Movers', 'New Bus Terminal', 'Chugal Pura Bus Station', 'Haji Camp Adda', 'Daewoo Express Bus Stand']

def generate_station_ids():
    id = random.randint(1, 10000)
    while (id in station_ids):
        id = random.randint(1, 10000)
    station_ids.append(id)
    return id
# Table No. 1: Stations
def assign_stations_to_cities(station_names, city_name):
    data = []
    for station in station_names:
        # Generate lat/lon around Karachi
        latitude = round(random.uniform(24.8, 25.1), 6)
        longitude = round(random.uniform(66.9, 67.1), 6)
        location = f"{station}, {city_name}"
        station_id = generate_station_ids()
        station_ids.append(station_id)
        data.append([station_id, station, location, city_name, latitude, longitude])

    station_df = pd.DataFrame(data, columns=['station_id', 'station_name', 'location', 'city', 'latitude', 'longitude'])
    return station_df
def generate_stations_table():
    lhr_stations_df = assign_stations_to_cities(station_names_lhr, 'Lahore')
    khi_stations_df = assign_stations_to_cities(station_names_khi, 'Karachi')
    isl_stations_df = assign_stations_to_cities(station_names_isl, 'Islamabad')
    que_stations_df = assign_stations_to_cities(station_names_que, 'Quetta')
    psh_stations_df = assign_stations_to_cities(station_names_psh, 'Peshawar')
    station_df = pd.concat([khi_stations_df, lhr_stations_df, isl_stations_df, que_stations_df, psh_stations_df], ignore_index=True)
    return station_df
    

commuter_ids = []
def generate_commuter_ids():
    id = random.randint(1, 10000)
    while (id in commuter_ids):
        id = random.randint(1, 10000)
    commuter_ids.append(id)
    return id

# Table No. 2: Commuters
def generate_commuter_table(n=50):
    data = []
    for _ in range(n):
        commuter_id = generate_commuter_ids()
        full_name = faker.name()
        phone = faker.phone_number()
        birth_date = faker.date_of_birth(minimum_age=15, maximum_age=85)
        gender = random.choices(['M', 'F', 'O', 'm', 'f', None], weights=[48, 48, 2, 48, 48, 2])[0]
        occupations = ['Teacher', 'Student', 'Doctor', 'Business Owner', 'Accountant', 'Housewife', 'Unemployed', 'Retired', 'N/A']
        weights = [15, 20, 8, 10, 7, 12, 10, 8, 7]
        occupation = random.choices(occupations, weights=weights, k=1)
        data.append([commuter_id, full_name, phone, birth_date, gender, occupation])

    commuter_df = pd.DataFrame(data, columns=['commuter_id', 'full_name', 'phone', 'birth_date', 'gender', 'occupation'])

    # Inject 5% invalid phones
    n_badphones = int(0.05 * n)
    bad_ids = np.random.choice(commuter_df['commuter_id'], n_badphones, replace=False)
    commuter_df.loc[commuter_df['commuter_id'].isin(bad_ids), 'phone'] = 'BADPHONE'


    return commuter_df

# Table No. 3: Routes
route_ids = []
def generate_route_ids():
    id = random.randint(1, 10000)
    while (id in route_ids):
        id = random.randint(1, 10000)
    route_ids.append(id)
    return id

# ---------- Route Table ----------
def generate_routes_table(station_df, n= 10 ):
    station_ids = station_df['station_id'].tolist()
    # station_info = station_df.set_index('station_id')[['station_name', 'city']].to_dict('index')
    station_info = (
    station_df.groupby('station_id')[['station_name', 'city']]
    .first()
    .to_dict('index')
    )
    data = []
    for i in range(1, n + 1):
        route_id = generate_route_ids()
        # Pick two different stations
        origin, destination = random.sample(station_ids, 2)
        origin_info = station_info[origin]
        dest_info = station_info[destination]
        origin_city = origin_info['city']
        dest_city = dest_info['city']
        origin_name = origin_info['station_name']
        dest_name = dest_info['station_name']
        # Intra-city or inter-city distance
        if origin_city == dest_city:
            distance = float(round(random.uniform(3, 30), 2))
        else:
            distance = float(round(random.uniform(30, 500), 2))
        # print(distance)
        # Route name format: CITYCODE-Station1→Station2
        city_code = origin_city[:3].upper()
        route_name = f"{city_code}-{origin_name}→{dest_name}"

        data.append([route_id, route_name, origin, destination, distance])

    return pd.DataFrame(data, columns=[
        'route_id', 'route_name', 'origin_station_id', 'destination_station_id', 'distance'
    ])

# ---------- Vehicle Table (4)----------
vehicle_ids = []
def generate_vehicle_ids():
    id = random.randint(1, 10000)
    while (id in vehicle_ids):
        id = random.randint(1, 10000)
    vehicle_ids.append(id)
    return id
def generate_vehicle_table(station_df, n=10):
    city_codes = station_df['city'].unique().tolist()
    bus_models = {
    'Toyota Coaster': 'Toyota',
    'Hino Dutro': 'Hino',
    'Daewoo BF106': 'Daewoo',
    'Isuzu Journey': 'Isuzu',
    'Volvo B8R': 'Volvo',
    'MAN Lion’s City': 'MAN',
    'Hyundai County': 'Hyundai',
    'Yutong ZK6129': 'Yutong'
    }
    train_models = {
    'Hitachi Class 800': 'Hitachi',
    'Bombardier Aventra': 'Bombardier',
    'Siemens Desiro': 'Siemens',
    'Alstom Coradia': 'Alstom',
    'Hyundai Rotem': 'Hyundai Rotem',
    'GE U30C': 'GE Transportation'
    }
    vehicle_types = ['Bus', 'Train', 'bus', 'train']
    data = []
    for _ in range(n):
        vehicle_id = generate_vehicle_ids()
        vehicle_type = random.choice(vehicle_types)
        if vehicle_type == 'Bus':
            capacity = random.randint(20, 50)
        else: 
            capacity = random.randint(100, 300)
        city = random.choice(city_codes)
        city_code = city[:3].upper()
        vehicle_number = f"{city_code}-{random.randint(1000, 9999)}"
        if vehicle_type == 'Bus' or vehicle_type == 'bus':
            model = random.choice(list(bus_models.keys()))
            manufacturer = bus_models[model]
        else:
            model = random.choice(list(train_models.keys()))
            manufacturer = train_models[model]
        manufacture_year = random.randint(1960, 2022)
        data.append([vehicle_id, vehicle_number, vehicle_type, capacity, manufacturer, manufacture_year, model])
        
    vehicle_df = pd.DataFrame(data, columns=['vehicle_id', 'vehicle_number', 'vehicle_type', 'capacity', 'manufacturer', 'manufacture_year', 'model']) 
    return vehicle_df

# 05: Vehicle Maintenance
vehicle_maintenance_ids = []
def generate_vehicle_maintenance_ids():
    id = random.randint(1, 10000)
    while (id in vehicle_maintenance_ids):
        id = random.randint(1, 10000)
    vehicle_maintenance_ids.append(id)
    return id
def generate_vehicle_maintenance_table(vehicle_df, n=5000):
    """Generate maintenance logs for vehicles."""
    data = []
    for i in range(1, n+1):
        vehicle_maintenance_id = generate_vehicle_maintenance_ids()
        vid = random.choice(vehicle_df['vehicle_id'].tolist())
        data.append([
            vehicle_maintenance_id,
            vid,
            faker.date_between(start_date=datetime(2019, 1, 1), end_date=datetime(2024, 1, 1)).strftime('%Y-%m-%d'), # maintenance date
            random.choice(['Engine', 'Tires', 'Brakes', 'Oil Change']), # maintenance type
            round(random.uniform(5000, 50000), 2), # cost
            faker.name() # performed by
        ])
    return pd.DataFrame(data, columns=['vehicle_maintenance_id', 'vehicle_id', 'maintenance_date', 'maintenance_type', 'cost', 'performed_by'])

# ---------- Ticket Office Table (06)----------
ticket_office_ids = []
def generate_ticket_office_ids():
    id = random.randint(1, 10000)
    while (id in ticket_office_ids):
        id = random.randint(1, 10000)
    ticket_office_ids.append(id)
    return id
def generate_ticket_office_table(num, station_df):
    data = []
    for i in range(1, num+1):
        ticket_office_id = generate_ticket_office_ids()
        name = faker.company()
        station_id = random.choice(station_df['station_id'].tolist())
        staff_count = random.randint(10, 30)
        data.append([ticket_office_id, name, station_id, staff_count])

    ticket_office_df = pd.DataFrame(data, columns=['ticket_office_id', 'name', 'station_id', 'staff_count'])
    return ticket_office_df

# 07: Date Table
def generate_date_table(n_years=5):
    """Generate Date table with holidays randomly flagged."""
    base = pd.date_range(start='2019-01-01', end=f'{2019 + n_years}-12-31')
    data = []
    for d in base:
        data.append([
            d.strftime('%Y-%m-%d'),  # Date_id
            d.day, # day
            d.month, # month
            d.year, # year
            d.strftime('%A'), # weekday
            random.choice([True, False, False, False])  # isHoliday
        ])
    return pd.DataFrame(data, columns=['date_id', 'Day', 'Month', 'Year', 'Weekday', 'isHoliday'])

# 08: TimeSlot Table with hourly buckets
def generate_timeslot_table():
    """Generate TimeSlot table with hourly buckets."""
    data = []
    for hour in range(24):
        for minute in [0, 30]:
            timeslot_id = f"{hour:02}{minute:02}"
            if 6 <= hour < 12:
                bucket = "Morning"
            elif 12 <= hour < 18:
                bucket = "Afternoon"
            elif 18 <= hour < 22:
                bucket = "Evening"
            else:
                bucket = "Night"
            data.append([timeslot_id, hour, minute, bucket])
    return pd.DataFrame(data, columns=['timeslot_id', 'hour', 'minute', 'time_bucket'])

# 09: Weather Table
weather_ids = []
def generate_weather_ids():
    id = random.randint(1, 10000)
    while (id in weather_ids):
        id = random.randint(1, 10000)
    weather_ids.append(id)
    return id
def generate_weather_table(n=50):
    """Generate synthetic weather data."""
    cities = ['Karachi', 'Lahore', 'Islamabad', 'Quetta', 'Peshawar', 'khi', 'psh', 'lhr', 'que', 'isl']
    data = []
    for i in range(1, n+1):
        weather_id = generate_weather_ids()
        city = random.choice(cities)
        temp = round(random.uniform(10, 40), 1)
        precip = round(random.uniform(0, 20), 1)
        condition = random.choice(['Sunny', 'Rainy', 'Cloudy', 'Stormy', 'Foggy'])
        data.append([weather_id, faker.date_between(start_date=datetime(2019, 1, 1), end_date=datetime(2024, 1, 1)).strftime('%Y-%m-%d'),
                     temp, precip, city, condition])
    return pd.DataFrame(data, columns=['WeatherID', 'Date', 'Temperature', 'Precipitation', 'City', 'Condition'])

# 10: Event Table
event_ids = []
def generate_event_ids():
    id = random.randint(1, 10000)
    while (id in event_ids):
        id = random.randint(1, 10000)
    event_ids.append(id)
    return id
def generate_event_table(n=10):
    event_names = ['City Parade', 'Music Festival', 'Transport Strike', 'Public Rally', 'Marathon', 'N/A', 'No event']
    event_types = ['Festival', 'Strike', 'Parade', 'Marathon', 'Political', 'N/A', 'No event']
    data = []
    for i in range(1, n+1):
        event_id = generate_event_ids()
        name = random.choice(event_names)
        date = faker.date_between(start_date=datetime(2019, 1, 1), end_date=datetime(2024, 1, 1)).strftime('%Y-%m-%d')
        city = random.choice(['Karachi', 'Lahore', 'Islamabad', 'Quetta', 'Peshawar'])
        e_type = random.choice(event_types)
        data.append([event_id, name, date, city, e_type])
    
    return pd.DataFrame(data, columns=['event_id', 'event_name', 'event_date', 'city', 'event_type'])

# ----------- Ticket Table (11)-------------------
ticket_ids = []
def generate_ticket_ids():
    id = random.randint(1, 10000)
    while (id in ticket_ids):
        id = random.randint(1, 10000)
    ticket_ids.append(id)
    return id
def generate_ticket_table(ticket_office_df, commuter_df, route_df, n=100):
    ticket_types = ['Single Ride', 'Return', 'Day Pass', 'Weekly Pass', 'Monthly Pass']
    payment_methods = ['Cash', 'Card', 'Mobile Wallet', 'CASH', 'CARD']
    data = []
    for i in range(1, n+1):
        ticket_id = generate_ticket_ids()
        ticket_office_id = random.choice(ticket_office_df['ticket_office_id'].tolist())
        commuter_id = random.choice(commuter_df['commuter_id'].tolist())
        route_id = random.choice(route_df['route_id'].tolist())
        route_distance = float(route_df.loc[route_df['route_id'] == route_id, 'distance'].values[0])
        
        if route_distance < 30:
            fare = round(random.uniform(100, 1000), 2)
        else: 
            fare = round(random.uniform(2000, 10000), 2)
        purchase_date = faker.date_between(start_date=datetime(2019, 1, 1), end_date=datetime(2024, 1, 1)).strftime('%Y-%m-%d')
        ticket_type = random.choice(ticket_types)
        payment_method = random.choice(payment_methods)
        data.append([ticket_id, commuter_id, route_id, ticket_office_id, fare, purchase_date, ticket_type, payment_method])
    
    return pd.DataFrame(data, columns=[
        'ticket_id', 'commuter_id', 'route_id', 'ticket_office_id',
        'fare', 'purchase_date', 'ticket_type', 'payment_method'
    ])

# 12: Ride Table
ride_ids = []
def generate_ride_ids():
    id = random.randint(1, 10000)
    while (id in ride_ids):
        id = random.randint(1, 10000)
    ride_ids.append(id)
    return id
def generate_rides_table(commuter_df, route_df, vehicle_df, station_df, date_df, timeslot_df, ticket_df, n=100):
    data = []
    for i in range(1, n+1):
        ride_id = generate_ride_ids()
        commuter_id = random.choice(commuter_df['commuter_id'].tolist())
        route_id = random.choice(route_df['route_id'].tolist())
        vehicle_id = random.choice(vehicle_df['vehicle_id'].tolist())
        stations = station_df['station_id'].sample(2).tolist()
        start_station, end_station = sorted(stations)
        date_id = random.choice(date_df['date_id'].tolist())
        time_id = random.choice(timeslot_df['timeslot_id'].tolist())
        ticket = ticket_df.sample(1).iloc[0]
        ticket_id = ticket['ticket_id']
        fare = ticket['fare']
        
        distance_row = route_df[route_df['route_id'] == route_id]
        if distance_row.empty:
            distance = 0.0
        else:
            distance = float(distance_row['distance'].iloc[0])

        # Generate duration based on random speed
        MIN_SPEED = 30
        MAX_SPEED = 100
        speed = np.random.randint(MIN_SPEED, MAX_SPEED)
        duration = round((distance / speed) * 60, 1)
        data.append({
            'RideID': ride_id,
            'CommuterID': commuter_id,
            'route_id': route_id,
            'VehicleID': vehicle_id,
            'Start_StationID': start_station,
            'End_StationID': end_station,
            'DateID': date_id,
            'TimeID': time_id,
            'ticket_id': ticket_id,
            'FareAmount': fare,
            'Duration': duration,
            'distance': distance
        })
    
    ride_df = pd.DataFrame(data)
    return ride_df

# adding issues to stations table
def mess_up_stations(station_df):
    # Add some missing values
    station_df.loc[station_df.sample(frac=0.03).index, 'latitude'] = np.nan
    station_df.loc[station_df.sample(frac=0.03).index, 'longitude'] = np.nan
    
    # Introduce string in numeric columns
    station_df['latitude'] = station_df['latitude'].astype(object)
    station_df.loc[station_df.sample(frac=0.01).index, 'latitude'] = 'unknown'
    
    # Duplicate rows
    duplicates = station_df.sample(frac=0.01)
    station_df = pd.concat([station_df, duplicates], ignore_index=True)
    
    return station_df

# adding issues to commuters table
def mess_up_commuters(commuter_df):
    # Nulls in gender and birth_date
    commuter_df.loc[commuter_df.sample(frac=0.02).index, 'gender'] = None
    commuter_df.loc[commuter_df.sample(frac=0.02).index, 'birth_date'] = None

    # Future birth dates (logic error)
    commuter_df.loc[commuter_df.sample(frac=0.01).index, 'birth_date'] = datetime(2030, 1, 1)

    # Non-list occupation type
    commuter_df.loc[commuter_df.sample(frac=0.01).index, 'occupation'] = 999

    # Typo in names
    commuter_df.loc[commuter_df.sample(frac=0.01).index, 'full_name'] = '???'
    
    return commuter_df

# adding issues to routes table
def mess_up_routes(routes_df):
    # Set zero distances (logical error for intercity)
    routes_df.loc[routes_df.sample(frac=0.02).index, 'distance'] = 0

    # Negative distances
    routes_df.loc[routes_df.sample(frac=0.01).index, 'distance'] = -20

    # Missing values
    routes_df.loc[routes_df.sample(frac=0.02).index, 'route_name'] = None

    return routes_df

def mess_up_rides(rides_df):
    # Set zero fares
    rides_df.loc[rides_df.sample(frac=0.02).index, 'FareAmount'] = 0

    # Negative distances
    rides_df.loc[rides_df.sample(frac=0.01).index, 'FareAmount'] = -20
    
    # Negative duration
    rides_df.loc[rides_df.sample(frac=0.01).index, 'Duration'] = -7
    # Missing values
    rides_df.loc[rides_df.sample(frac=0.02).index, 'FareAmount'] = None


    return rides_df

# ----------- Generate Tables -----------
date_df = generate_date_table()
timeslot_df = generate_timeslot_table()
events_df = generate_event_table()
station_df = generate_stations_table()
stations_df = mess_up_stations(station_df)
ticket_office_df = generate_ticket_office_table(5000, stations_df)
route_df = generate_routes_table(stations_df)
routes_df = mess_up_routes(route_df)
commuter_df = generate_commuter_table()
commuters_df = mess_up_commuters(commuter_df)
vehicle_df = generate_vehicle_table(stations_df)
tickets_df = generate_ticket_table(ticket_office_df, commuters_df, routes_df)
vehicle_maint_df = generate_vehicle_maintenance_table(vehicle_df)
weather_df = generate_weather_table()
rides_df = generate_rides_table(commuters_df, routes_df, vehicle_df, stations_df, date_df, timeslot_df, tickets_df)
ride_df = mess_up_rides(rides_df)
# Save all dataframes to preview
output = {
    "Date Table": date_df.head(),
    "TimeSlot Table": timeslot_df.head(),
    "Commuter Table": commuter_df.head(),
    "Vehicle Table": vehicle_df.head(),
    "Vehicle Maintenance Table": vehicle_maint_df.head(),
    "Station Table": station_df.head(),
    "Ticket Office Table": ticket_office_df.head(),
    "Ticket Table": tickets_df.head(),
    "Event Table": events_df.head(),
    "Route Table": routes_df.head(),
    "Ride Table": ride_df.head(),
    "Weather Table": weather_df.head()
}
vehicle_df.to_csv("vehicle_data.csv", mode='a', index=False, header=False)
vehicle_maint_df.to_csv("maintenance_data.csv",  mode='a', index=False, header=False)
ticket_office_df.to_csv("ticket_office_data.csv",  mode='a', index=False, header=False)
timeslot_df.to_csv("timeslot_data.csv",  mode='a', index=False, header=False)
routes_df.to_csv("routes_data.csv",  mode='a', index=False, header=False)
ride_df.to_csv("rides_data.csv", mode='a', index=False, header=False)
date_df.to_csv("date_data.csv",  mode='a', index=False, header=False)
weather_df.to_csv("weather_data.csv",  mode='a', index=False, header=False)
events_df.to_csv("events_data.csv",  mode='a', index=False, header=False)
tickets_df.to_csv("tickets.csv",  mode='a', index=False, header=False)
stations_df.to_csv("station_data.csv", mode='a', index=False, header=False)
commuters_df.to_csv("commuter_data.csv",  mode='a', index=False, header=False)
# output
for table_name, df in output.items():
    print(f"\n==== {table_name} ====")
    print(df)


