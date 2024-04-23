# -*- coding: utf-8 -*-
"""
Created on Tue Jan  9 14:59:38 2024

@author: Harsh
"""

import time
from zk import ZK, const
import pyodbc
from multiprocessing import Process

device_configs = [
    {'ip': '192.168.2.251', 'port': 4370},
    {'ip': '192.168.2.252', 'port': 4370},
    {'ip': '192.168.2.253', 'port': 4370}
]

server = 'zimyo_sql'
database = 'zimyo_attendance'
username = 'abravmsd'
password = 'password'
table_name = 'AttendanceTable'

def connect_to_device(device_config):
    zk = ZK(device_config['ip'], device_config['port'], timout=5, password=0, force_udp=False, ommit_ping=False)
    try:
        zk.connect()
        return zk
    except Exception as e:
        print(f"Error connecting to {device_config['ip']}:{device_config['port']}: {e}")
        return None
    
def connect_to_sql_server():
    try:
        conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")
        return None

def create_attendance_table(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE TABLE {table_name} (DeviceID INT, UserID INT, VerifyMode INT, InOutMode INT, AttTime DATETIME)")
        conn.commit()
        print(f"Attendance table created successfully.")
    except pyodbc.ProgrammingError:
        print(f"Table {table_name} already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()
        
def insert_attendance_record(conn, record):
    cursor = conn.cursor()
    try:
        cursor.execute(f"INSERT INTO {table_name} (DeviceID, UserID, VerifyMode, InOutMode, AttTime) VALUES (?, ?, ?, ?, ?)", record)
        conn.commit()
    except Exception as e:
        print(f"Error inserting record: {e}")
    finally:
        cursor.close()

def read_and_update_attendance(devices, conn):
    create_attendance_table(conn)

    while True:
        for device_id, device_config in enumerate(device_configs, start=1):
            zk_device = connect_to_device(device_config)
            if zk_device:
                try:
                    attendance = zk_device.get_attendance()
                    if attendance:
                        for record in attendance:
                            record_with_device_id = (device_id,) + record
                            insert_attendance_record(conn, record_with_device_id)
                except Exception as e:
                    print(f"Error reading attendance from {device_config['ip']}:{device_config['port']}: {e}")
                finally:
                    zk_device.disconnect()

        time.sleep(60)  # Adjust the interval based on your needs

if __name__ == "__main__":
    sql_conn = connect_to_sql_server()

    if sql_conn:
        try:
            read_and_update_attendance(device_configs, sql_conn)
        except KeyboardInterrupt:
            print("Script terminated by user.")
        finally:
            sql_conn.close()

#run in background            
def process_device(device_id, device_config, conn):
    zk_device = connect_to_device(device_config)
    if zk_device:
        try:
            while True:
                attendance = zk_device.get_attendance()
                if attendance:
                    for record in attendance:
                        record_with_device_id = (device_id,) + record
                        insert_attendance_record(conn, record_with_device_id)
                time.sleep(60)  # Adjust the interval based on your needs
        except Exception as e:
            print(f"Error reading attendance from {device_config['ip']}:{device_config['port']}: {e}")
        finally:
            zk_device.disconnect()

def run_processes(device_configs, conn):
    processes = []
    for device_id, device_config in enumerate(device_configs, start=1):
        process = Process(target=process_device, args=(device_id, device_config, conn))
        process.start()
        processes.append(process)

    try:
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        print("Script terminated by user.")
    finally:
        for process in processes:
            process.terminate()

if __name__ == "__main__":
    sql_conn = connect_to_sql_server()

    if sql_conn:
        try:
            run_processes(device_configs, sql_conn)
        finally:
            sql_conn.close()
