import json
import os

def generate_config(
        config_file_name: str,
        path_exp: str,
        path_data: str,
        path_data_dir: str,
        seed: int,
        visual,
        embedding: dict,
        clustering: dict,
        clustering_mode: str,
        save_plots: bool
):
    config = {
        'path_exp': path_exp,
        'path_embeddings': path_data,
        'path_data_dir': path_data_dir,
        'seed': seed,
        'embedding': embedding,
        'ml_posts_and_image_analysis': clustering,
        'mode': clustering_mode,
        'visual': visual,
        'save_plots': save_plots,
    }

    # DA FINIRE, IO QUA METTEREI IL NOME DEL MODELLO

    config_file_path = os.path.join("configurations/ml_posts_and_image_analysis")

    if not os.path.exists(config_file_path):
        os.makedirs(config_file_path)

    with open(os.path.join(config_file_path, config_file_name), "w") as config_file:
        json.dump(config, config_file, indent=4)

    print(f"Configuration file {config_file_path} generated and saved.")


def experiment_main():
    # ------------------- CHANGE THIS -----------------------
    config_file_name = "clustering_euclidean.json"

    # preprocess
    path_exp = "results/ml_posts_and_image_analysis"

    path_data = "embeddings/distiluse-base-multilingual-cased-v2"

    path_data_dir = "data/username_squat_data/posts/twitter"

    seed = 1233

    visual = {
        "method": "tsne",
        "also_cosine": True
    }

    save_plots = True

    # embedding
    embedding = {
        # "identity": {},
        "umap": {
            "metric": "cosine",
            "n_neighbors": 15,
            "min_dist": 0.0,
            "n_components": 5,
            "random_state": 42
        }
    }

    # methods for ml_posts_and_image_analysis, possibility: ['full', 'centroid']
    clustering_mode = 'full'

    # metric for ml_posts_and_image_analysis, possibility: ['cosine', 'dot']
    clustering_metric = 'euclidean'

    # ml_posts_and_image_analysis
    clustering = {
        'hdbscan': {'clustering_metric': clustering_metric,
                    'min_samples': 5,
                    'min_cluster_size': 50},
        # 'kmeans': {'clustering_metric': clustering_metric,
        #            'n_clusters': 4,
        #            "n_init": "auto"},
        # 'fast_clustering': {
        #     'clustering_metric': clustering_metric,
        #     'min_community_size': 20,
        #     'threshold': 0.6},
        # "agglomerative": {
        #     "clustering_metric": clustering_metric,
        #     "linkage": "average",
        #     "distance_threshold": 0.55}
    }

    # ----------------------------------------------------

    generate_config(
        config_file_name,
        path_exp,
        path_data,
        path_data_dir,
        seed,
        visual,
        embedding,
        clustering,
        clustering_mode,
        save_plots
    )


if __name__ == "__main__":
    experiment_main()
