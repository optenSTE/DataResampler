# -*- coding: utf-8 -*-

"""
Программа усредняет данные во входных файлах с указанным интервалом

Ождаемый формат входных файлов (разделитель - табулятор)
Первый столбец - UNIX время (секунды с долями)
Остальные - любые числовые значения, количество столбцов должно быть одинаковым во всех файлах
1575352800.0615	1500.9582	1505.1311	1509.4126	1512.6564
1575352800.1615	1500.9582	1505.1309	1509.4122	1512.6566

"""

import pandas as pd
import datetime
import glob
import configparser
import sys
import os


def time_parser(time_in_secs):
    ret = None
    try:
        ret = datetime.datetime.fromtimestamp(float(time_in_secs))  # utcfromtimestamp
    except:
        ret = 0
    return ret


# default ini-values
resample_interval_sec = 600
files_template = '*.txt'
in_file_timestamp_type = 'unix'
in_file_header = None
in_file_delimiter = '\t'
out_file_header = False
out_file_delimiter = '\t'

config = configparser.ConfigParser()
try:
    filename, file_extension = os.path.splitext(sys.argv[0])
    ini_file_name = f"{filename}.ini"
    config.read(ini_file_name)

    resample_interval_sec = float(config['main']['resample_interval_sec'])
    files_template = config['main']['in_files_template']

    in_file_timestamp_type = config['input_file']['timestamp_format']
    in_file_header = int(config['input_file']['header'])
    if in_file_header == 0:
        in_file_header = None
    # in_file_delimiter = config['input_file']['delimiter']

    out_file_header = config['output_file'].getboolean('header')
    # in_file_delimiter = config['output_file']['delimiter']

    pass

except Exception as e:
    print(f'Error during ini-file reading: {str(e)}')
    pass

print(f'resample_interval_sec = {resample_interval_sec}')
print(f'files_template = {files_template}')


file_list = glob.glob(files_template)
bigdata = None
print('Reading files...')
for in_csv_file_name in file_list:
    print(in_csv_file_name + "...")
    try:
        if in_file_timestamp_type.upper() == "UNIX":
            # for UNIX timestamp
            df = pd.read_csv(in_csv_file_name, sep=in_file_delimiter, header=in_file_header, index_col=0, parse_dates=[0], date_parser=time_parser, error_bad_lines=False)
        else:
            # for 20.05.2020 12:37:32.11343
            df = pd.read_csv(in_csv_file_name, sep=in_file_delimiter, header=in_file_header, index_col=0, parse_dates=[0], converters={'time': lambda t: datetime.datetime.strptime(t, in_file_timestamp_type).time()}, error_bad_lines=False)

        if type(bigdata) is not pd.DataFrame:
            bigdata = df
        else:
            bigdata = bigdata.append(df)
    except Exception as e:
        print(f'Error during file reading: {str(e)}')

print(f'ok, read {len(file_list)} files')

# resample
if type(bigdata) is pd.DataFrame:
    print('Resampling...')
    df_resampled = bigdata.resample(f'{resample_interval_sec}S').mean()
    print('ok')

    # remove empty rows
    df_resampled.dropna(axis=0, how='any', thresh=None, subset=None, inplace=False)

    # save to CSV file
    print('Saving...')
    out_excel_file_name = f'resample_{resample_interval_sec}_sec.txt'
    df_resampled.to_csv(out_excel_file_name, header=out_file_header, sep=out_file_delimiter, encoding='utf-8')
    print('ok')
else:
    print('Nothing to save')
