import data_to_db.functions as fc

chunk_path = "./data/parts_of_data"
chunk_path_in_container = '/tmp/'
DOCKER_NAME = 'ml_way-db-1'
DB_name = 'students_depression'
if __name__ == '__main__':
    i = 1
    fc.create_db(DB_name)
    while True:
        if fc.check_file_exists(chunk_path, f'chunk_{i}.csv'):
            command = f'''LOAD DATA INFILE '/tmp/chunk_{i}.csv' INTO TABLE student_data FIELDS TERMINATED BY ',' ENCLOSED BY "'"LINES TERMINATED BY '\n'IGNORE 1 ROWS;'''
            fc.docker_cp(chunk_path, i, DOCKER_NAME)
            fc.execute_mysql_command(command, database=DB_name)
            fc.docker_rm(chunk_path_in_container, i, DOCKER_NAME)
            i += 1
