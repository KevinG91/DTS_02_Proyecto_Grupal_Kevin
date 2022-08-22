import psycopg2

#creating the connection
def get_connection():
    connection = psycopg2.connect(
        host = "localhost",
        database = "test",
        user = "postgres",
        password = "123123")
    return connection
#closing the connection, always do it
def close_connection(connection):
    if connection:
        connection.close()

#creating the database
def create_table():
    connection = get_connection()
    cursor = connection.cursor()
    query = """CREATE TABLE statistics_city_wide(
	    OccurrencePrecinctCode INTEGER,
	    GeoCodeLabel VARCHAR(50),
	    Borough VARCHAR(50),
	    Number_of_Motor_Vehicle_Collisions INTEGER,
	    Vehicles_or_Motorists_Involved INTEGER,
	    Injury_or_Fatal_Collisions INTEGER,
	    MotoristsInjured INTEGER,
	    MotoristsKilled INTEGER,
	    PassengInjured INTEGER,
	    PassengKilled INTEGER,
	    CyclistsInjured INTEGER,
	    CyclistsKilled INTEGER,
	    PedestrInjured INTEGER,
	    PedestrKilled INTEGER,
	    Bicycle INTEGER);"""
    cursor.execute(query)
    connection.commit()
    close_connection(connection)
#loading the CSV
def load_statistics_city_wide():
    connection = get_connection()
    cursor = connection.cursor()
    query = """COPY statistics_city_wide(
        OccurrencePrecinctCode,
		GeoCodeLabel,
		Borough,
		Number_of_Motor_Vehicle_Collisions,
        Vehicles_or_Motorists_Involved,
		Injury_or_Fatal_Collisions,
		MotoristsInjured,
		MotoristsKilled,
		PassengInjured,
		PassengKilled,
		CyclistsInjured,
		CyclistsKilled,
		PedestrInjured,
		PedestrKilled,
		Bicycle)
    FROM 'C:/Users/Kevin/Desktop/Clases Henry/Proyecto_Grupal_Temp/DTS_02_Proyecto_Grupal_Kevin/report_city_wide_statistics.csv'
    DELIMITER ','
    ;"""
    cursor.execute(query)
    connection.commit()
    close_connection(connection)
#executing the query to sum each column containing the number of injured
def number_of_injured():
    connection = get_connection()
    cursor = connection.cursor()
    query = """SELECT SUM(MotoristsInjured), SUM(PassengInjured), SUM(CyclistsInjured), SUM(PedestrInjured) FROM statistics_city_wide"""
    cursor.execute(query)
    results = cursor.fetchall()
    close_connection(connection)
    return results[0]

def number_of_killed():
    connection = get_connection()
    cursor = connection.cursor()
    query = """SELECT SUM(MotoristsKilled), SUM(PassengKilled), SUM(CyclistsKilled), SUM(PedestrKilled) FROM statistics_city_wide"""
    cursor.execute(query)
    results = cursor.fetchall()
    close_connection(connection)
    return results[0]

def show_number_of_injured():
    injured = number_of_injured()
    print(f"Motorists Injured: {injured[0]}\nPassengers Injured: {injured[1]}\nCyclists Injured: {injured[2]}\nPedestrians Injured: {injured[3]}")

def show_number_of_killed():
    killed = number_of_killed()
    print(f"Motorists killed: {killed[0]}\nPassengers killed: {killed[1]}\nCyclists killed: {killed[2]}\nPedestrians killed: {killed[3]}")

create_table()
load_statistics_city_wide()
show_number_of_injured()
show_number_of_killed()
