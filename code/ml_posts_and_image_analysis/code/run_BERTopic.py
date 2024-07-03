# python experiments/run_BERTopic.py --config=configurations/BERTopic/15_0.8_50_clustering_euclidean_full.json

import argparse
import os
import sys
from transformers import set_seed

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from src.utils.load_embeddings import *
from exp_utils import *

from sklearn.feature_extraction.text import CountVectorizer

from bertopic import BERTopic


# ------------------------------------

LOAD_CENTROIDS_FILES = False
SAVE_CENTROIDS_FILES = False
SAVE_IN_PICKLE = True

# ------------------------------------


DEVICE = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")


def exits_centroids_path(path_embeddings, path_data_dir):
    complete_path  = os.path.join(args['path_embeddings'], args['path_data_dir'])
    complete_path = complete_path.split('/')[1:]
    complete_path.insert(0, "saved_centroids")
    complete_path = os.path.join(*complete_path)

    if os.path.exists(complete_path):
        return True
    else:
        return False


def main():
    parser = argparse.ArgumentParser(description='Run experiments based on a configuration file.')
    parser.add_argument('--config', type=str, help='Path to the configuration file')

    args = parser.parse_args()

    config_file_path = args.config

    args = read_config_file(config_file_path)

    print(args)

    set_seed(args['seed'])

    print(f"Using {DEVICE}")

    if LOAD_CENTROIDS_FILES and args['mode'] == 'centroid':
        if exits_centroids_path(args['path_embeddings'], args['path_data_dir']):
            info_embeddings, embeddings, sentences, titles = load_only_centroids_files(args['path_embeddings'], args['path_data_dir'])
        else:
            info_embeddings, embeddings, sentences, titles = load_embeddings_and_sentences(args['path_embeddings'], args['path_data_dir'], args['mode'], also_titles=True)
    else:
        info_embeddings, embeddings, sentences, titles = load_embeddings_and_sentences(args['path_embeddings'], args['path_data_dir'], args['mode'], also_titles=True)
    if SAVE_CENTROIDS_FILES and args['mode'] == 'centroid':
        if not exits_centroids_path(args['path_embeddings'], args['path_data_dir']):
            save_only_centroids_files(args['path_embeddings'], args['path_data_dir'], info_embeddings, embeddings, sentences, titles)

    # get Sentence-Embedder name
    model_name = args["path_embeddings"].split('/')[1]
    model = get_sentence_model(model_name, device=DEVICE)

    # get embedding method
    embedding_name = "umap"
    embedding_args = args['BERTopic']["umap"]
    embedding_obj = def_decomposition(embedding_name, **embedding_args)
    embedding_method = embedding_obj.get_runner()

    # get ml_posts_and_image_analysis method
    clustering_name = "hdbscan"
    clustering_args = args['BERTopic']["hdbscan"]
    clustering_obj = def_clustering(clustering_name, DEVICE, **clustering_args)
    clustering_method = clustering_obj.get_runner()

    # set vectorizer model
    if args['BERTopic']["vectorizer"]["use_vectorizer"]:
        vectorizer_args = args['BERTopic']["vectorizer"]["args"]
        vectorizer_args["ngram_range"] = tuple(vectorizer_args["ngram_range"])
        vectorizer_model = CountVectorizer(**vectorizer_args)
    else:
        vectorizer_model = None
    
    # get representation_model
    representation_model = def_representation_model(args['BERTopic']['representation'])

    topic_model = BERTopic(

        # Pipeline models
        embedding_model=model,
        umap_model=embedding_method,
        hdbscan_model=clustering_method,
        vectorizer_model=vectorizer_model,
        representation_model=representation_model,

        # Hyperparameters
        top_n_words=args['BERTopic']["hyperparameters"]["top_n_words"],
        verbose=args['BERTopic']["hyperparameters"]["verbose"]
    )

    topics, probs = topic_model.fit_transform(sentences, embeddings.numpy())

    print(topic_model.get_topic_info())

    super_folder = generate_folders(f"experiments/{args['path_exp']}", parse_super_folder(clustering_obj.name, args))

    topic_model.save(f"{super_folder}/topic_model", serialization="safetensors", save_ctfidf=True, save_embedding_model=model)
    if SAVE_IN_PICKLE:
        topic_model.save(f"{super_folder}/pickle_topic_model.pkl", serialization="pickle", save_ctfidf=True, save_embedding_model=model)
    
    serialize(topics, f"{super_folder}/ori_topics.pk")
    serialize(probs, f"{super_folder}/ori_probs.pk")

    results = exp_get_results(info_embeddings, topics, args["mode"])
    results.to_csv(f'{super_folder}/results.csv', index=False)

    save_json(args, super_folder, "config.json")


if __name__ == "__main__":
    main()