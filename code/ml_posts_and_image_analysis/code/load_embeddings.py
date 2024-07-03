import os
import torch

from utilities import serialize, deserialize


def get_all_files_in_directory(directory):
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list


def load_matrix_embeddings(path_embeddings, clustering_mode):
    # file_names = [file for file in os.listdir(path_embeddings) if os.path.isfile(os.path.join(path_embeddings, file))]
    global embeddings
    file_names = [file for file in get_all_files_in_directory(path_embeddings) if os.path.isfile(file)]

    for i, file_name in enumerate(file_names):
        if i == 0:
            if clustering_mode == 'full':
                embeddings = torch.load(f'{file_name}')["embeddings"]
            elif clustering_mode == 'centroid':
                embeddings = torch.load(f'{file_name}')["mean"]
            else:
                raise "Clustering mode not defined"
        else:
            if clustering_mode == 'full':
                embeddings = torch.cat((embeddings, torch.load(f'{file_name}')["embeddings"]))
            elif clustering_mode == 'centroid':
                embeddings = torch.cat((embeddings, torch.load(f'{file_name}')["mean"]))
            else:
                raise "Clustering mode not defined"

    return embeddings


def load_embeddings(path_embeddings, path_data_dir, clustering_mode):
    # file_names = [file for file in os.listdir(path_embeddings) if os.path.isfile(os.path.join(path_embeddings, file))]
    # file_names = [file for file in get_all_files_in_directory(path_embeddings) if os.path.isfile(file)]

    complete_path = path_embeddings + '/' + path_data_dir
    file_names = [f for f in os.listdir(complete_path) if os.path.isfile(os.path.join(complete_path, f))]

    info_embeddings = []

    for i, file_name in enumerate(file_names):

        obj_emb = torch.load(f'{file_name}')

        if i == 0:
            if clustering_mode == 'full':
                embeddings = obj_emb["embeddings"]
            elif clustering_mode == 'centroid':
                embeddings = obj_emb["mean"]
            else:
                raise "Clustering mode not defined"
        else:
            if clustering_mode == 'full':
                embeddings = torch.cat((embeddings, obj_emb["embeddings"]))
            elif clustering_mode == 'centroid':
                embeddings = torch.cat((embeddings, obj_emb["mean"]))
            else:
                raise "Clustering mode not defined"

        if clustering_mode == 'full':
            info_embeddings.append((obj_emb['tot_idx'], obj_emb['csv_path'], obj_emb['file_name']))
        elif clustering_mode == 'centroid':
            info_embeddings.append((-1, obj_emb['csv_path'], obj_emb['file_name']))
        else:
            raise "Clustering mode not defined"

        del obj_emb

    return info_embeddings, embeddings


def load_embeddings_and_sentences(path_embeddings, path_data_dir, clustering_mode, also_titles=False):
    # file_names = [file for file in os.listdir(path_embeddings) if os.path.isfile(os.path.join(path_embeddings, file))]
    # file_names = [file for file in get_all_files_in_directory(path_embeddings) if os.path.isfile(file)]

    complete_path = path_embeddings + '/' + path_data_dir
    file_names = [f for f in os.listdir(complete_path) if os.path.isfile(os.path.join(complete_path, f))]

    info_embeddings = []
    sentences = []
    titles = []

    for i, file_name in enumerate(file_names):

        obj_emb = torch.load(complete_path + '/' + file_name)

        if i == 0:
            if clustering_mode == 'full':
                embeddings = obj_emb["embeddings"]
            elif clustering_mode == 'centroid':
                embeddings = obj_emb["mean"]
            else:
                raise "Clustering mode not defined"
        else:
            if clustering_mode == 'full':
                embeddings = torch.cat((embeddings, obj_emb["embeddings"]))
            elif clustering_mode == 'centroid':
                embeddings = torch.cat((embeddings, obj_emb["mean"]))
            else:
                raise "Clustering mode not defined"

        if clustering_mode == 'full':
            info_embeddings.append((obj_emb['tot_idx'], obj_emb['csv_path'], obj_emb['file_name']))
        elif clustering_mode == 'centroid':
            info_embeddings.append((-1, obj_emb['csv_path'], obj_emb['file_name']))
        else:
            raise "Clustering mode not defined"

        if clustering_mode == 'full':
            sentences.extend(obj_emb["sentences"])
        elif clustering_mode == 'centroid':
            if obj_emb["keywords_list"] is None:
                sentences.append(obj_emb["file_name"])
            else:
                sentences.append(obj_emb["keywords_list"])
        else:
            raise "Clustering mode not defined"

        if also_titles:
            if clustering_mode == 'full':
                titles.extend([obj_emb["file_name"] for i in range(len(obj_emb["sentences"]))])
            elif clustering_mode == 'centroid':
                titles.append(obj_emb["file_name"])
            else:
                raise "Clustering mode not defined"

        del obj_emb

    return info_embeddings, embeddings, sentences, titles


def load_embeddings_and_sentences_only_centroids(path_embeddings, path_data_dir, also_titles=False):
    complete_path = path_embeddings + '/' + path_data_dir
    complete_path = complete_path.split('/')[1:]
    complete_path.insert(0, "embeddings_centroids")
    complete_path = os.path.join(*complete_path)
    assert os.path.exists(complete_path)

    file_names = [f for f in os.listdir(complete_path) if os.path.isfile(os.path.join(complete_path, f))]

    info_embeddings = []
    sentences = []
    titles = []

    for i, file_name in enumerate(file_names):

        obj_emb = torch.load(complete_path + '/' + file_name)

        if i == 0:
            embeddings = obj_emb["mean"]
        else:
            embeddings = torch.cat((embeddings, obj_emb["mean"]))

        info_embeddings.append((-1, obj_emb['csv_path'], obj_emb['file_name']))

        if obj_emb["keywords_list"] is None:
            sentences.append(obj_emb["file_name"])
        else:
            sentences.append(obj_emb["keywords_list"])

        if also_titles:
            titles.append(obj_emb["file_name"])

        del obj_emb

    return info_embeddings, embeddings, sentences, titles


def save_only_centroids_files(path_embeddings, path_data_dir, info_embeddings, embeddings, sentences, titles):
    complete_path = path_embeddings + '/' + path_data_dir
    complete_path = complete_path.split('/')[1:]
    complete_path.insert(0, "saved_centroids")
    complete_path = os.path.join(*complete_path)

    if not os.path.exists(complete_path):
        os.makedirs(complete_path)

    serialize(info_embeddings, os.path.join(complete_path, 'info_embeddings.pkl'))
    serialize(embeddings, os.path.join(complete_path, 'embeddings.pkl'))
    serialize(sentences, os.path.join(complete_path, 'sentencess.pkl'))
    serialize(titles, os.path.join(complete_path, 'titles.pkl'))


def load_only_centroids_files(path_embeddings, path_data_dir):
    complete_path = path_embeddings + '/' + path_data_dir
    complete_path = complete_path.split('/')[1:]
    complete_path.insert(0, "saved_centroids")
    complete_path = os.path.join(*complete_path)
    assert os.path.exists(complete_path)

    info_embeddings = deserialize(os.path.join(complete_path, 'info_embeddings.pkl'))
    embeddings = deserialize(os.path.join(complete_path, 'embeddings.pkl'))
    sentences = deserialize(os.path.join(complete_path, 'sentencess.pkl'))
    titles = deserialize(os.path.join(complete_path, 'titles.pkl'))

    return info_embeddings, embeddings, sentences, titles
