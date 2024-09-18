from mysql.connector import Error
import mysql.connector
import pandas as pd
from datetime import datetime
import os
from datetime import datetime


def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='labiot_data_sensed',
            user='root',
            password='Ibiza123'
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def close_connection(connection):
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")


def check_if_table_exist(table_name, connection):
    cursor = connection.cursor()
    try:
        if table_name == 'Activity':
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id_sensed INT AUTO_INCREMENT PRIMARY KEY, sensor_id INT, date TIMESTAMP, value_sensed TINYINT)" 
        else:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id_sensed INT AUTO_INCREMENT PRIMARY KEY, sensor_id INT, date TIMESTAMP, value_sensed FLOAT)"
        cursor.execute(create_table_query)
        connection.commit()
            
    except Error as e:
        print(f"Error: {e}")

def insert_data_to_table(table_name, sensor_id, date, value_sensed, connection):
    cursor = connection.cursor()
    try:
        insert_query = f"INSERT INTO {table_name} (sensor_id, date, value_sensed) VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (sensor_id, date, value_sensed))
        connection.commit()
    except Error as e:
        print(f"Error: {e}")

def insert_new_sensor(sensor_name, connection):
    cursor = connection.cursor()
    try:
        # Check if the sensor already exists
        select_query = "SELECT COUNT(*) FROM sensors WHERE name = %s"
        cursor.execute(select_query, (sensor_name,))
        result = cursor.fetchone()
        
        if result[0] == 0:  # If the sensor does not exist
            insert_query = "INSERT INTO sensors (name) VALUES (%s)"
            cursor.execute(insert_query, (sensor_name,))
            connection.commit()
        else:
            print(f"Sensor '{sensor_name}' already exists.")
    except Error as e:
        print(f"Error: {e}")

def get_sensor_id(sensor_name, connection):
    cursor = connection.cursor()
    try:
        select_query = "SELECT sensor_id FROM sensors WHERE name = %s"
        cursor.execute(select_query, (sensor_name,))
        result = cursor.fetchone()
        return result[0]
    except Error as e:
        print(f"Error: {e}")
        return None 

def insert_docs_already_open(doc_name, connection):
    cursor = connection.cursor()
    try:
        insert_query = "INSERT INTO docs_already_open (name) VALUES (%s)"
        cursor.execute(insert_query, (doc_name,))
        connection.commit()   
    except Error as e:
        print(f"Error: {e}")
        
def check_if_doc_already_open(doc_name, connection):
    cursor = connection.cursor()
    try:
        select_query = "SELECT COUNT(*) FROM docs_already_open WHERE name = %s"
        cursor.execute(select_query, (doc_name,))
        result = cursor.fetchone()
        return result[0] > 0
    except Error as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    csv_directory = r'C:\Users\Fernando Esqueda\Desktop\pip-24.2'
    conn = connect_to_database()
    if conn:
        for file_name in os.listdir(csv_directory):
            if file_name.startswith('report-202') and file_name.endswith('.csv'):
                
                if check_if_doc_already_open(file_name, conn):
                    print(f"Archivo {file_name} ya fue procesado, omitiendo.")
                    continue
                # Cargar el archivo CSV
                df = pd.read_csv(file_name)
                # Se obtienen los nombres de las columnas, que son los dispositivos y las caracteristicas que miden
                devices_sensed = df.columns.tolist()[1:]

                # Se define un diccionario empty
                device_feature_sensed_per_device = {}

                # iterar sobre cada dispositivo y característica
                for device in devices_sensed:
                    
                    ## Aqui vamos a guardar el nombre de cada dispositivo y la caracteristica que se esta midiendo, para tener el nombre de la caracteristica
                    device_feature_sensed = []
                    # se guarda el nombre del dispositivo y la característica que se está midiendo
                    current_device = device.split('/ ')[0]
                    feature = device.split('/ ')[1]
                    if feature == 'Battery Voltage':
                        feature = 'Battery_voltage'
                    
                    insert_new_sensor(current_device, conn)
                    
                    # Se itera sobre cada renglon dentro de la columna con el nombre del dispositivo
                    for index, item in df[device].items():
                        #Se verifica si no es un valor NaN, porque si es asi, no lo necesitamos, lo ignoramos
                        if pd.notna(item): 
                            # Se convierte la fecha de string a objeto datetime y después a string con formato SQL
                            fecha_str = df['time'][index]
                            fecha_obj = datetime.strptime(fecha_str, "%a, %d %b %Y %H:%M:%S")
                            fecha_sql = fecha_obj.strftime("%Y-%m-%d %H:%M:%S")
                            # Se guarda el nombre del dispositivo, la fecha y el valor de la característica
                            device_feature_sensed.append([current_device, fecha_sql, item])
                    
                    # Si ya se encuentra la palabra clave en el diccionario, se añaden los valores, si no, se crea una nueva entrada
                    if feature in device_feature_sensed_per_device:
                        device_feature_sensed_per_device[feature].extend(device_feature_sensed)
                    else:
                        device_feature_sensed_per_device[feature] = device_feature_sensed
                insert_docs_already_open(file_name, conn)
                for measures, values in device_feature_sensed_per_device.items():

                    check_if_table_exist(measures, conn)
                    sensor_id = get_sensor_id(values[0][0], conn)
                    for value in values:
                        insert_data_to_table(measures, sensor_id, value[1], value[2], conn)

        close_connection(conn)