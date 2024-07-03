# python experiments/analyze_BERTopic.py --config=configurations/BERTopic/15_0.8_50_clustering_euclidean_full.json

import os
import sys
from umap import UMAP
from transformers import set_seed

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from load_embeddings import *
from exp_utils import *
from utils_analyze_BERTopic import *

# ------------------------------------

PATH_RESULT = "experiments/results/BERTopic/all-mpnet-base-v2/hdbscan/full/23-01-2024_04-57-57"

# ------------------------------------
# SAVE IN DIR
# ------------------------------------

PATH_SAVE_ANALYSIS = PATH_RESULT + '/analysis'

# ------------------------------------
# SETTINGS
# ------------------------------------

UMAP_REDUCED_ARGS = {
    "n_neighbors": 10,
    "n_components": 2,
    "min_dist": 0.0,
    "metric": 'cosine',
    "random_state": 42
}

LOAD_CENTROIDS_FILES = True
LOAD_IN_PICKLE = True

# ------------------------------------

REPRESENTATION_MODEL = 'KeyBERT'
N_TOP = 5

# ------------------------------------

NOT_LANG_LIST = ['th', 'ja', 'ar', 'ru', 'zh', 'ko']

# ------------------------------------
# ------------------------------------

DEVICE = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")


def main():
    print(f"Using {DEVICE}")

    args = read_config_file(PATH_RESULT + '/config.json')
    print(args)

    set_seed(args['seed'])

    path_embeddings = args['path_embeddings']
    path_data_dir  = args['path_data_dir']
    mode = args['mode']

    path_reducted_embeddings = path_embeddings + '/' + path_data_dir + "/reduced_embeddings"

    model_name = path_embeddings.split('/')[1]

    if LOAD_CENTROIDS_FILES and args['mode'] == 'centroid':
        info_embeddings, embeddings, sentences, titles = load_only_centroids_files(args['path_embeddings'], args['path_data_dir'])
    else:
        info_embeddings, embeddings, sentences, titles = load_embeddings_and_sentences(args['path_embeddings'], args['path_data_dir'], args['mode'], also_titles=True)

    if os.path.exists(path_reducted_embeddings + f'/reduced_embeddings_{mode}.npy'):
        reduced_embeddings = np.load(path_reducted_embeddings + f'/reduced_embeddings_{mode}.npy')
    else:
        reduced_embeddings = UMAP(**UMAP_REDUCED_ARGS).fit_transform(embeddings)
        if not os.path.exists(path_reducted_embeddings): os.makedirs(path_reducted_embeddings)
        np.save(path_reducted_embeddings + f'/reduced_embeddings_{mode}.npy', reduced_embeddings)
        save_json(UMAP_REDUCED_ARGS, path_reducted_embeddings, f"config_{mode}.json")

    ori_topics = deserialize(f"{PATH_RESULT}/ori_topics.pk")
    ori_probs = deserialize(f"{PATH_RESULT}/ori_probs.pk")

    BERTopic.my_hierarchical_topics = my_hierarchical_topics

    date = generate_time()

    # ---------------------------------------
    # PART COMPLETE ORIGINAL CLUSTERS
    # ---------------------------------------

    print("PART COMPLETE ORIGINAL CLUSTERS")

    upper_folder_complete = generate_folders(PATH_SAVE_ANALYSIS, [date, 'complete'])
    topic_model, embedding_model = load_topic_model(model_name, device=DEVICE, path_result=PATH_RESULT, load_in_pickle=LOAD_IN_PICKLE)

    # Use REPRESENTATION_MODEL for better comprehension
    representation_topic_labels = {topic: " | ".join(filter_unique_english_words(list(zip(*values))[0], n_top=N_TOP, not_lang_list=NOT_LANG_LIST)) for topic, values in topic_model.topic_aspects_[REPRESENTATION_MODEL].items()}
    topic_model.set_topic_labels(representation_topic_labels)

    save_visualize_bertopic(topic_model, titles, sentences, reduced_embeddings, upper_folder_complete, n_top=N_TOP, representation_model=REPRESENTATION_MODEL, load_in_pickle=LOAD_IN_PICKLE)

    # ---------------------------------------
    # PART REDUCTION OUTLIERS
    # ---------------------------------------

    print("PART REDUCTION OUTLIERS")

    upper_folder_reducted = generate_folders(PATH_SAVE_ANALYSIS, [date, 'reducted'])
    topic_model, embedding_model = load_topic_model(model_name, device=DEVICE, path_result=PATH_RESULT, load_in_pickle=LOAD_IN_PICKLE)

    # Use REPRESENTATION_MODEL for better comprehension
    representation_topic_labels = {topic: " | ".join(filter_unique_english_words(list(zip(*values))[0], n_top=N_TOP, not_lang_list=NOT_LANG_LIST)) for topic, values in topic_model.topic_aspects_[REPRESENTATION_MODEL].items()}
    topic_model.set_topic_labels(representation_topic_labels)

    # Reduce outliers with pre-calculate embeddings instead
    new_topics = topic_model.reduce_outliers(sentences, ori_topics, strategy="embeddings", embeddings=embeddings.numpy())
    topic_model.update_topics(sentences, topics=new_topics)

    results = exp_get_results(info_embeddings, new_topics, args["mode"])
    results.to_csv(f'{upper_folder_reducted}/reducted_results.csv', index=False)

    save_visualize_bertopic(topic_model, titles, sentences, reduced_embeddings, upper_folder_reducted, n_top=N_TOP, representation_model=REPRESENTATION_MODEL, load_in_pickle=LOAD_IN_PICKLE)

    # ---------------------------------------


if __name__ == "__main__":
    main()