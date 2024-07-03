import json
import os


# link="pairwise"

def generate_config(
        config_file_name: str,
        path_exp: str,
        path_data: str,
        path_data_dir: str,
        seed: int,
        BERTopic: dict,
        clustering_mode: str,
        save_plots: bool,
        save_pk: bool
):
    config = {
        'path_exp': path_exp,
        'path_embeddings': path_data,
        'path_data_dir': path_data_dir,
        'seed': seed,
        'BERTopic': BERTopic,
        'mode': clustering_mode,
        'save_plots': save_plots,
        'save_pk': save_pk
    }

    config_file_path = os.path.join("configurations/BERTopic")

    if not os.path.exists(config_file_path):
        os.makedirs(config_file_path)

    with open(os.path.join(config_file_path, config_file_name), "w") as config_file:
        json.dump(config, config_file, indent=4)

    print(f"Configuration file {config_file_path} generated and saved.")


def experiment_main():
    config_file_name = "clustering_euclidean_centroid.json"

    # preprocess
    path_exp = "results/BERTopic"

    path_data = "embeddings/distiluse-base-multilingual-cased-v2"

    path_data_dir = "data/username_squat_data/posts/twitter"

    seed = 1233

    save_plots = True

    # --------------------------------
    # representation models
    keybert_model = {}

    pos_model = {
        "model": "en_core_web_sm"
    }

    mmr_model = {
        "diversity": 0.3
    }

    use_vectorizer = True

    vectorizer = {
        "use_vectorizer": use_vectorizer,
        "args": {
            "stop_words": "english",
            "min_df": 2,
            "ngram_range": (1, 2)
        }
    }

    BERTopic = {
        "umap": {
            "metric": "cosine",
            "n_neighbors": 15,
            "min_dist": 0.0,
            "n_components": 5,
            "random_state": 42
        },
        "hdbscan": {
            "clustering_metric": "euclidean",
            "min_samples": 10,
            "min_cluster_size": 10
        },
        "representation": {
            "keybert_model": keybert_model,
            "pos_model": pos_model,
            "mmr_model": mmr_model
        },
        "hyperparameters": {
            "top_n_words": 10,
            "verbose": True
        },
        "vectorizer": vectorizer
    }

    # methods for ml_posts_and_image_analysis, possibility: ['full', 'centroid']
    clustering_mode = 'centroid'

    save_pk = True

    generate_config(
        config_file_name,
        path_exp,
        path_data,
        path_data_dir,
        seed,
        BERTopic,
        clustering_mode,
        save_plots,
        save_pk
    )


if __name__ == "__main__":
    experiment_main()
