import os
import pandas as pd
import csv
import sys
import pycld2 as cld2
import json
import ast


sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from src.utils.utilities import *
from src.utils.plots import *



normalized = lambda x: (x-np.min(x))/(np.max(x)-np.min(x))


def read_config_file(config_file_path):
    with open(config_file_path, "r") as config_file:
        config_data = json.load(config_file)
    return config_data


def save_json(file, folder_name, file_name):
    with open(os.path.join(folder_name, file_name), "w") as config_file:
        json.dump(file, config_file, indent=4)


def parse_super_folder(model_name: str, args: dict) -> list:
    nlp_name = args['path_embeddings'].split('/')[-1]
    mode = args['mode']
    date = generate_time()

    return [nlp_name, model_name, mode, date]


def exp_get_results_full_embeddings(info_embeddings: list, yhat: np.array):
    data = []

    for tpl in info_embeddings:
        tot_idx, csv_path, file_name = tpl
        subset_array = yhat[:tot_idx]
        yhat = yhat[tot_idx:]
        
        for i, value in enumerate(subset_array):
            data.append((i, csv_path, file_name, value))

    df = pd.DataFrame(data, columns=['idx', 'csv_path', 'file_name', 'label'])

    return df


def exp_get_results_centroid_embeddings(info_embeddings: list, yhat: np.array):
    data = []

    for i, tpl in enumerate(info_embeddings):
        centroid_idx, csv_path, file_name = tpl

        data.append((centroid_idx, csv_path, file_name, yhat[i]))

    df = pd.DataFrame(data, columns=['idx', 'csv_path', 'file_name', 'label'])

    return df


def exp_get_results(info_embeddings: list, yhat: np.array, clustering_mode: str):
    if clustering_mode == 'full': df = exp_get_results_full_embeddings(info_embeddings, yhat)
    elif clustering_mode == 'centroid': df = exp_get_results_centroid_embeddings(info_embeddings, yhat)
    else:
        raise "Clustering mode not implemented"
    
    return df


def exp_run_clustering(model: ClusteringAlgorithm, clustering_mode: str, info_embeddings: list, embeddings: np.array):
    # model.reset()

    yhat = model.forward(embeddings)

    results = exp_get_results(info_embeddings, yhat, clustering_mode)

    return yhat, results


def exp_save_plots(X, yhat, visual = None, upper_folder = None):
    if visual is not None:
        plots_folder = f'{upper_folder}/plots'
        standard_plot(X, yhat, visual["method"], plots_folder)
    if visual["also_cosine"] and visual["method"] in ["tsne", "umap"]:
        cosine_plot(X, yhat, visual["method"], plots_folder)


def read_csv(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        data = list(reader)
    return header, data


def write_csv(file_path, header, data):
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(data)


def write_csv_cluster(file_path, data):
    with open(file_path, 'w', newline='') as file_csv:
        writer = csv.writer(file_csv)
        writer.writerows(map(lambda x: [x], data))


def extract_sentences_cluster(file_path, content, max_post_per_files=None, new_template=True, not_unk_post=False, only_eng_post=True):
    if new_template:
        sentences = content.split("----end-----")
        sentences = [s.strip().replace("----start-----\n", "").replace(";", "").replace('\n', ' ') for s in sentences if
                     s.strip()]
    else:
        sentences = content.split("-------- line separator --------")
        sentences = [s.strip().replace(";", "").replace('\n', ' ') for s in sentences if s.strip()]

    sentences_dict = {"idx": [], "csv_path": [], "sentence": []}

    for idx, sentence in enumerate(sentences[:max_post_per_files]):
        try:
            if not_unk_post:
                isReliable, textBytesFound, details = cld2.detect(sentence)
                if details[0][1] == "un":
                    continue

            if only_eng_post:
                isReliable, textBytesFound, details = cld2.detect(sentence)
                if details[0][1] != "en":
                    continue

        except Exception as e:
            continue

        sentences_dict['idx'].append(idx)
        sentences_dict['csv_path'].append(file_path)
        sentences_dict['sentence'].append(sentence)

    return sentences_dict


def get_keywords(data, cluster_id, col_index_idx, col_index_keybert, n_keywords=5):
    str_keyword = None

    for riga in data:
        if riga[col_index_idx] == cluster_id:
            keywords = [elem_list for elem_list in ast.literal_eval(riga[col_index_keybert])][:n_keywords]
            str_keyword = ' | '.join(keywords)
            break
    return str_keyword