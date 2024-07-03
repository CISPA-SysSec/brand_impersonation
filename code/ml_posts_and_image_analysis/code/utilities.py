import numpy as np
import os
import datetime
import re
import pickle
import dill

from sentence_transformers import SentenceTransformer
from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance, PartOfSpeech
from code.ml.code.clustering_agglomerative import ClusteringAgglomerative
from code.ml.code.clustering_fast_clustering import ClusteringFastClustering
from code.ml.code.clustering_hdbscan import ClusteringHdbscan
from code.ml.code.clustering_kmeans import ClusteringKmeans
from code.ml.code.projection_identity import ProjectIdentity
from code.ml.code.projection_pca import ProjectPCA
from code.ml.code.projection_tsne import ProjectTSNE
from code.ml.code.projection_umap import ProjectUMAP


def serialize(data, name, use_dill=False):
    """Store data (serialize)"""
    if use_dill:
        with open(name, 'wb') as handle:
            dill.dump(data, handle, protocol=dill.HIGHEST_PROTOCOL)
    else:
        with open(name, 'wb') as handle:
            pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize(name, use_dill=False):
    """Load data (deserialize)"""
    if use_dill:
        with open(name, 'rb') as handle:
            unserialized_data = dill.load(handle)
    else:
        with open(name, 'rb') as handle:
            unserialized_data = pickle.load(handle)
    return unserialized_data


def generate_folders(base_path, folders):
    for folder in folders:
        base_path = os.path.join(base_path, folder)
        if not os.path.exists(base_path):
            os.makedirs(base_path)

    return base_path


def generate_time():
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%d-%m-%Y_%H-%M-%S")
    experiment_name = f"{formatted_time}"

    # Replace characters that may interfere with file manager
    experiment_name = re.sub(r"[:]", "_", experiment_name)

    return experiment_name


def flatten_reshape(X: np.array) -> np.array:
    n_samples, n_features, n_channels = X.shape
    X_reshaped = X.reshape(n_samples, n_features * n_channels)

    return X_reshaped


def shuffle_indexes(idxs):
    return idxs[np.random.permutation(len(idxs))]


def def_decomposition(method: str, cosine_sim=False, **kwarg):
    methods = {
        "tsne": ProjectTSNE,
        "umap": ProjectUMAP,
        "pca": ProjectPCA
    }

    if cosine_sim:
        kwarg['metric'] = "cosine"

    if method in list(methods.keys()):
        proj_alg = methods[method](**kwarg)
    else:
        proj_alg = ProjectIdentity(**kwarg)

    # if cosine_sim:
    #     cosine_sim_matrix = cosine_similarity(X)
    #     X_emb = proj_alg.forward(cosine_sim_matrix)
    # else:
    #     X_emb = proj_alg.forward(X)

    # X = normalized(X)

    return proj_alg


def def_clustering(method: str, device=None, **kwargs):
    methods = {
        "kmeans": ClusteringKmeans,
        "hdbscan": ClusteringHdbscan,
        "fast_clustering": ClusteringFastClustering,
        "agglomerative": ClusteringAgglomerative
    }

    if method in list(methods.keys()):
        model = methods[method](device=device, **kwargs)
    else:
        raise "Clustering method not implemented"
    return model


def get_sentence_model(model_name, device=None):
    model = SentenceTransformer(model_name, device=device)

    return model


def def_representation_model(representation_dict: dict):
    representation_model = {}

    representation_models_names = list(representation_dict.keys())

    if "keybert_model" in representation_models_names:
        keybert_args = representation_dict["keybert_model"]
        keybert_model = KeyBERTInspired(**keybert_args)
        representation_model["KeyBERT"] = keybert_model

    if "pos_model" in representation_models_names:
        pos_args = representation_dict["pos_model"]
        # python3 -m spacy download en_core_web_sm
        pos_model = PartOfSpeech(**pos_args)
        representation_model["pos_model"] = pos_model

    # MMR
    if "mmr_model" in representation_models_names:
        mmr_args = representation_dict["mmr_model"]
        mmr_model = MaximalMarginalRelevance(**mmr_args)
        representation_model["mmr_model"] = mmr_model

    return representation_model
