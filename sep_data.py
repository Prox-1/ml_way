from data_to_db.functions import data_separation

data_path = "./data/student_depression_dataset.csv"
chunk_path = "./data/parts_of_data"

data_separation(data_path, chunk_path, 3000, 2)
