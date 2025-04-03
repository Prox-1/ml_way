import math
import subprocess
import time
from pathlib import Path

import mysql.connector
import pandas as pd


def docker_cp(chunk_path, i, docker_name):
    try:
        command = [
            'docker',
            'cp',
            f'{chunk_path}/chunk_{i}.csv',
            f'{docker_name}:/tmp'
        ]
        result = subprocess.run(
            command, capture_output=True, text=True, check=True)
        if result.returncode == 0:
            print(
                f"Файл {chunk_path}/chunk_{i}.csv успешно скопирован в {docker_name}:/tmp")
        else:
            print(f"Ошибка при копировании файла: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении команды docker cp: {e}")
    except FileNotFoundError:
        print("Ошибка: Команда 'docker' не найдена. Убедитесь, что Docker установлен и доступен в PATH.")
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")


def docker_rm(chunk_path, i, docker_name):
    try:
        command = [
            'docker',
            'exec',
            '-i',
            '-t',
            docker_name,
            'rm',
            f'{chunk_path}/chunk_{i}.csv'
        ]
        result = subprocess.run(
            command, capture_output=True, text=True, check=True)
        if result.returncode == 0:
            print(
                f"Файл {chunk_path}/chunk_{i}.csv успешно удален из {docker_name}:/tmp")
        else:
            print(f"Ошибка при копировании файла: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении команды docker cp: {e}")
    except FileNotFoundError:
        print("Ошибка: Команда 'docker' не найдена. Убедитесь, что Docker установлен и доступен в PATH.")
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")


def execute_mysql_command(command, host="127.0.0.1", user='root', password='root', database='students_depression', type='other'):
    """
    Выполняет SQL-команду на сервере MySQL, находящемся в Docker-контейнере.

    Args:
        host (str): IP-адрес или имя хоста Docker-контейнера.
        user (str): Имя пользователя MySQL.
        password (str): Пароль пользователя MySQL.
        database (str): Имя базы данных, к которой нужно подключиться.
        command (str): SQL-команда, которую нужно выполнить.

    Returns:
        list: Список результатов запроса (если это SELECT-запрос).
    """
    try:
        if type == 'other':
            mydb = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
        elif type == 'create_db':
            mydb = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
            )
        mycursor = mydb.cursor()
        mycursor.execute(command)
        if command.lower().startswith("select"):
            results = mycursor.fetchall()
            mydb.close()
            return results
        else:
            mydb.commit()
            mydb.close()
            return None
    except mysql.connector.Error as err:
        print(f"Ошибка: {err}")
        return None


def check_file_exists(directory, filename):
    filepath = Path(directory) / filename  # Создаем объект Path для файла
    return filepath.exists() and filepath.is_file()


def data_separation(data_path, chunk_path, chunk_size, t_interval):

    data = pd.read_csv(data_path)
    n_parts = math.ceil(data.shape[0] / chunk_size)

    for i in range(n_parts):
        start_index = i * chunk_size

        end_index = min((i + 1) * chunk_size, len(data))
        df_chunk = data[start_index:end_index]
        output_file = f'{chunk_path}/chunk_{i + 1}.csv'
        df_chunk.to_csv(output_file, index=False)
        print(f'Сохранен файл: {output_file}')
        time.sleep(t_interval)


def create_db(DB_name='students_depression'):
    command_db_create = f'CREATE DATABASE {DB_name}'
    execute_mysql_command(command_db_create, type='create_db')
    command_table_create = '''CREATE TABLE `student_data` (
  `id` int NOT NULL,
  `Gender` varchar(10) DEFAULT NULL,
  `Age` decimal(5,2) DEFAULT NULL,
  `City` varchar(25) DEFAULT NULL,
  `Profession` varchar(25) DEFAULT NULL,
  `Academic_Pressure` decimal(10,2) DEFAULT NULL,
  `Work_Pressure` decimal(10,2) DEFAULT NULL,
  `CGPA` decimal(10,2) DEFAULT NULL,
  `Study_Satisfaction` decimal(10,2) DEFAULT NULL,
  `Job_Satisfaction` decimal(10,2) DEFAULT NULL,
  `Sleep_Duration` varchar(25) DEFAULT NULL,
  `Dietary_Habits` varchar(25) DEFAULT NULL,
  `Degree` varchar(25) DEFAULT NULL,
  `had_suicidal_thoughts` varchar(25) DEFAULT NULL,
  `Work_Study_Hours` decimal(10,2) DEFAULT NULL,
  `Financial_Stress` varchar(25) DEFAULT NULL,
  `Family_History_of_Mental_Illness` varchar(25) DEFAULT NULL,
  `Depression` int DEFAULT NULL,
  PRIMARY KEY (`id`))'''
    execute_mysql_command(command_table_create,
                          database=DB_name)
    print(f'База данных "{DB_name}" успешно создана')
