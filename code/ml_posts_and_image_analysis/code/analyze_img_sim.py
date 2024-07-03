# python experiments/analyze_img_sim.py

import os
import ast
import pandas as pd
from tqdm import tqdm

from exp_utils import read_csv, write_csv

# -------------------------

PATH_CSV_DATA = 'experiments/results/img_sim/CLIP-VIT-B-16/10-01-2024_18-03-58'

PATH_DATA_OFFICIAL_IMG = 'data_img/official'

# -------------------------

DO_CUT = True
THR_VALUE = 80
N_MAX_SCORE = 10

FILTER_FILE_LIST = ['official_useinsider']

# -------------------------


def create_thr_reduced_results(path_csv_data, thr_value, filter_file_list=None):
    folders_csv = [d for d in os.listdir(path_csv_data) if os.path.isdir(os.path.join(path_csv_data, d))]

    for folder in tqdm(folders_csv, desc="Folder"):
        thr_folder = os.path.join(path_csv_data, folder, f'THR_{thr_value}')
        if not os.path.exists(thr_folder):
            os.makedirs(thr_folder)

        csv_path = os.path.join(path_csv_data, folder, 'results_cut.csv')
        csv_path_out = os.path.join(thr_folder, f'thr_{thr_value}_results.csv')

        header, data = read_csv(csv_path)

        # Find indexes
        col_index_best_official_name = header.index('best_official_name')
        col_index_best_official_score = header.index('best_official_score')

        for riga in tqdm(data, desc="Rows"):
            if filter_file_list is not None and riga[col_index_best_official_name] in filter_file_list:
                riga[col_index_best_official_score] = -1
            if float(riga[col_index_best_official_score]) < thr_value:
                riga[col_index_best_official_score] = -1
                riga[col_index_best_official_name] = "miscellaneous"

        write_csv(csv_path_out, header, data)


def create_thr_reduced_summary_results(path_csv_data, thr_value):
    folders_csv = [d for d in os.listdir(path_csv_data) if os.path.isdir(os.path.join(path_csv_data, d))]

    for folder in tqdm(folders_csv, desc="Folder"):
        thr_folder = os.path.join(path_csv_data, folder, f'THR_{thr_value}')

        if not os.path.exists(thr_folder):
            print('ERROR, THR folder does not exist')
            exit()
        else:
            csv_path = os.path.join(thr_folder, f'thr_{thr_value}_results.csv')
            csv_path_out = os.path.join(thr_folder, f'thr_{thr_value}_results_summary.csv')

            df = pd.read_csv(csv_path)

            df_counts = df.groupby('best_official_name').size().reset_index(name='Counts')

            df_counts = df_counts.sort_values(by='Counts', ascending=False)

            df_counts['Percentage'] = (df_counts['Counts'] / df.shape[0]) * 100

        df_counts.to_csv(csv_path_out, index=False)


def create_thr_reduced_csv_logos_associations(path_csv_data, path_data_official_img, thr_value):
    folders_csv = [d for d in os.listdir(path_csv_data) if os.path.isdir(os.path.join(path_csv_data, d))]

    png_official_files = [os.path.join(root, file) for root, dirs, files in os.walk(path_data_official_img) for file in files if
                 file.endswith(".png")]

    for folder in tqdm(folders_csv, desc="Folder"):

        thr_folder = os.path.join(path_csv_data, folder, f'THR_{thr_value}')

        if not os.path.exists(thr_folder):
            print('ERROR, THR folder does not exist')
            exit()
        else:
            thr_official_folder = os.path.join(thr_folder, 'official_csv')
            if not os.path.exists(thr_official_folder):
                os.makedirs(thr_official_folder)

            csv_path = os.path.join(thr_folder, f'thr_{thr_value}_results.csv')

            header, data = read_csv(csv_path)

            col_index_file_name = header.index('file_name')
            col_index_best_official_name = header.index('best_official_name')
            col_index_best_official_score = header.index('best_official_score')

            for png_file in png_official_files:
                png_name = png_file.split('/')[-1].split('.')[:-1][0]

                csv_path_out = os.path.join(thr_official_folder, f'{png_name}.csv')

                header_official = ['account_name', 'score']
                data_official = []

                for riga in data:

                    if riga[col_index_best_official_name] == png_name:
                        data_official.append([riga[col_index_file_name], riga[col_index_best_official_score]])

                write_csv(csv_path_out, header_official, data_official)


def create_thr_results(path_csv_data, thr_value, path_data_official_img=None, filter_file_list=None):
    create_thr_reduced_results(path_csv_data, thr_value, filter_file_list)
    create_thr_reduced_summary_results(path_csv_data, thr_value)
    if path_data_official_img is not None:
        create_thr_reduced_csv_logos_associations(path_csv_data, path_data_official_img, thr_value)


def reduce_results(path_csv_data, n_max_score):
    folders_csv = [d for d in os.listdir(path_csv_data) if os.path.isdir(os.path.join(path_csv_data, d))]

    for folder in tqdm(folders_csv, desc="Folder"):
        csv_path = os.path.join(path_csv_data, folder, 'results.csv')
        csv_path_out = os.path.join(path_csv_data, folder, 'results_cut.csv')

        header, data = read_csv(csv_path)

        col_index = header.index('complete_score_list')

        for riga in tqdm(data, desc="Rows"):
            riga[col_index] = ast.literal_eval(riga[col_index])[:n_max_score]

        header[col_index] = 'score_list'

        write_csv(csv_path_out, header, data)


def summary_results(path_csv_data):
    folders_csv = [d for d in os.listdir(path_csv_data) if os.path.isdir(os.path.join(path_csv_data, d))]

    for folder in tqdm(folders_csv, desc="Folder"):
        csv_path = os.path.join(path_csv_data, folder, 'results_cut.csv')
        csv_path_out = os.path.join(path_csv_data, folder, 'results_summary.csv')

        df = pd.read_csv(csv_path)

        df_counts = df.groupby('best_official_name').size().reset_index(name='Counts')

        df_counts = df_counts.sort_values(by='Counts', ascending=False)

        df_counts['Percentage'] = (df_counts['Counts'] / df.shape[0]) * 100

        df_counts.to_csv(csv_path_out, index=False)


def main():
    if DO_CUT:
        print("Creating reduced version summary images similarity")
        reduce_results(path_csv_data=PATH_CSV_DATA, n_max_score=N_MAX_SCORE)
    print("Creating results summary")
    summary_results(path_csv_data=PATH_CSV_DATA)
    print("Analysis with threshold")
    create_thr_results(path_csv_data=PATH_CSV_DATA, thr_value=THR_VALUE, path_data_official_img=PATH_DATA_OFFICIAL_IMG, filter_file_list=FILTER_FILE_LIST)


if __name__ == "__main__":
    main()