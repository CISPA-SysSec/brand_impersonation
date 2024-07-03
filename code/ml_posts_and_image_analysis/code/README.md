##  Brand Impersonation: Analysis of Posts and Image of Fradulent Profiles Readme File

## Dependencies 

Please refer to environment.yml file for dependencies required for code usage.

## Image similarity analysis

The following are commands for performing image similarity analysis.

The default directory expected for storing the platform folders containing account images is "data_img". However, the location of the files inside the Python scripts can be changed.

1. To create the embeddings for the images:

    ```
    python create_img_embeddings.py
    ```
2. After having created the embeddings for the images, it is possible to compute the similarity between account profile images and original logos using:
    
    ```
    python experiments/run_img_sim.py
    ```

3. Finally, after changing the PATH_CSV_DATA variable with the path of the results obtained by the "run_img_sim.py" script, the following command generates the CSV results files:

    ```
    python experiments/analyze_img_sim.py
    ```

## Posts clustering analysis

The following are commands for performing posts clustering analysis.

The default directory expected to storing all the account's posts is "data/all". However, the location of the files inside the Python scripts can be changed.

1. To create the embeddings for the posts:

    ```
    python create_embeddings.py
    ```

2. After having created the embeddings for the posts, it is possible to run the BERTopic framework for clustering using:

    ```
    python experiments/run_BERTopic.py --config=configurations/BERTopic/15_0.8_50_clustering_euclidean_full.json
    ```

    where --config defines the location of the BERTopic JSON configuration file. To change the hyperparametrization for posts clustering analysis, create a JSON configuration file in "configurations/BERTopic."

3. Finally, after changing the PATH_RESULT variable with the path of the results obtained by the "run_BERTopic.py" script, the following command generates the CSV results files:

    ```
    python experiments/analyze_BERTopic.py --config=configurations/BERTopic/15_0.8_50_clustering_euclidean_full.json
    ```

    Again, --config defines the BERTopic JSON configuration file.

4. Additionally, after changing the PATH_CSV_DATA variable with the path of the results obtained by the "analyze_BERTopic.py" script, it is possible to generate extra CSV analysis files using:

    ```
    python experiments/extract_results_BERTopic.py
    ```