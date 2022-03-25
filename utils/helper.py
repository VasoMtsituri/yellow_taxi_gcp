import os


def open_file(filename):
    with open(filename, 'r') as f:
        data = f.read()

    return data


def write_into_file(filename, data):
    with open(filename, 'a') as f:
        f.write(data)


def list_all_files(directory):
    files = [f.name for f in os.scandir(directory) if f.is_file()]

    return files


def take_csv_2_upload(directory='./partitions'):
    csvs = list_all_files(directory)

    enumerated_csvs = [(index, csv) for index, csv in enumerate(csvs)]

    return enumerated_csvs


def indices_used(filename='indices.txt'):
    indices_str = open_file(filename)

    if ' ' in indices_str:
        indices = indices_str.split()
        indices = [int(x) for x in indices]
    else:
        return []

    return indices


def file_checker():
    csvs = take_csv_2_upload()
    indices = indices_used()

    for csv in csvs:
        if csv[0] not in indices:
            return csv


def save_index(**kwargs):
    filename = 'indices.txt'
    ti = kwargs['ti']

    write_into_file(filename, ti)
   