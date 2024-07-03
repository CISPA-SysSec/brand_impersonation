from typing import List, Callable
from scipy.sparse import csr_matrix
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from scipy.cluster import hierarchy as sch
from bertopic._utils import validate_distance_matrix
from tqdm import tqdm
from packaging import version
from sklearn import __version__ as sklearn_version

from sentence_transformers import SentenceTransformer
from bertopic import BERTopic
import pycld2 as cld2
import difflib


def my_hierarchical_topics(self,
                            docs: List[int],
                            linkage_function: Callable[[csr_matrix], np.ndarray] = None,
                            distance_function: Callable[[csr_matrix], csr_matrix] = None,
                            n_words: int = None,
                            representation_model: str = None) -> pd.DataFrame:

        if distance_function is None:
            distance_function = lambda x: 1 - cosine_similarity(x)

        if linkage_function is None:
            linkage_function = lambda x: sch.linkage(x, 'ward', optimal_ordering=True)

        # Calculate distance
        embeddings = self.c_tf_idf_[self._outliers:]
        X = distance_function(embeddings)
        X = validate_distance_matrix(X, embeddings.shape[0])

        # Use the 1-D condensed distance matrix as an input instead of the raw distance matrix
        Z = linkage_function(X)

        # Calculate basic bag-of-words to be iteratively merged later
        documents = pd.DataFrame({"Document": docs,
                                  "ID": range(len(docs)),
                                  "Topic": self.topics_})
        documents_per_topic = documents.groupby(['Topic'], as_index=False).agg({'Document': ' '.join})
        documents_per_topic = documents_per_topic.loc[documents_per_topic.Topic != -1, :]
        clean_documents = self._preprocess_text(documents_per_topic.Document.values)

        # Scikit-Learn Deprecation: get_feature_names is deprecated in 1.0
        # and will be removed in 1.2. Please use get_feature_names_out instead.
        if version.parse(sklearn_version) >= version.parse("1.0.0"):
            words = self.vectorizer_model.get_feature_names_out()
        else:
            words = self.vectorizer_model.get_feature_names()

        bow = self.vectorizer_model.transform(clean_documents)

        # Extract clusters
        hier_topics = pd.DataFrame(columns=["Parent_ID", "Parent_Name", "Topics",
                                            "Child_Left_ID", "Child_Left_Name",
                                            "Child_Right_ID", "Child_Right_Name"])
        for index in tqdm(range(len(Z))):

            # Find clustered documents
            clusters = sch.fcluster(Z, t=Z[index][2], criterion='distance') - self._outliers
            cluster_df = pd.DataFrame({"Topic": range(len(clusters)), "Cluster": clusters})
            cluster_df = cluster_df.groupby("Cluster").agg({'Topic': lambda x: list(x)}).reset_index()
            nr_clusters = len(clusters)

            # Extract first topic we find to get the set of topics in a merged topic
            topic = None
            val = Z[index][0]
            while topic is None:
                if val - len(clusters) < 0:
                    topic = int(val)
                else:
                    val = Z[int(val - len(clusters))][0]
            clustered_topics = [i for i, x in enumerate(clusters) if x == clusters[topic]]

            # Group bow per cluster, calculate c-TF-IDF and extract words
            grouped = csr_matrix(bow[clustered_topics].sum(axis=0))
            c_tf_idf = self.ctfidf_model.transform(grouped)
            selection = documents.loc[documents.Topic.isin(clustered_topics), :]
            selection.Topic = 0
            
            if representation_model is None or self.representation_model is None:
                words_per_topic = self._extract_words_per_topic(words, selection, c_tf_idf, calculate_aspects=False)
            else:
                representative_list = list(self.representation_model.keys())
                assert representation_model in representative_list
                words_per_topic = self._extract_words_per_topic(words, selection, c_tf_idf, calculate_aspects=True)
                words_per_topic = self.topic_aspects_[representation_model]

            # Extract parent's name and ID
            parent_id = index + len(clusters)
            parent_name = "_".join([x[0] for x in words_per_topic[0]][:n_words])

            # Extract child's name and ID
            Z_id = Z[index][0]
            child_left_id = Z_id if Z_id - nr_clusters < 0 else Z_id - nr_clusters

            if Z_id - nr_clusters < 0:
                child_left_name = "_".join([x[0] for x in self.get_topic(Z_id)][:n_words])
            else:
                child_left_name = hier_topics.iloc[int(child_left_id)].Parent_Name

            # Extract child's name and ID
            Z_id = Z[index][1]
            child_right_id = Z_id if Z_id - nr_clusters < 0 else Z_id - nr_clusters

            if Z_id - nr_clusters < 0:
                child_right_name = "_".join([x[0] for x in self.get_topic(Z_id)][:n_words])
            else:
                child_right_name = hier_topics.iloc[int(child_right_id)].Parent_Name

            # Save results
            hier_topics.loc[len(hier_topics), :] = [parent_id, parent_name,
                                                    clustered_topics,
                                                    int(Z[index][0]), child_left_name,
                                                    int(Z[index][1]), child_right_name]

        hier_topics["Distance"] = Z[:, 2]
        hier_topics = hier_topics.sort_values("Parent_ID", ascending=False)
        hier_topics[["Parent_ID", "Child_Left_ID", "Child_Right_ID"]] = hier_topics[["Parent_ID", "Child_Left_ID", "Child_Right_ID"]].astype(str)

        return hier_topics

# ------------------------------------

def load_topic_model(model_name, device, path_result, load_in_pickle=False):
    embedding_model = SentenceTransformer(model_name, device=device)

    # Load model and add embedding model
    if load_in_pickle:
        topic_model = BERTopic.load(path_result + "/pickle_topic_model.pkl", embedding_model=embedding_model)
    else:
        topic_model = BERTopic.load(path_result + "/topic_model", embedding_model=embedding_model)

    return topic_model, embedding_model

# ------------------------------------

def get_topic_rep_docs(bert_model):
    """
    Desc:
        - takes in the bert model and gets the model assigned representative
        docs per each topic
    Inputs
        - bert_model: the model which contains the get_topics() fn
    Returns:
        - all_topics_rep: dict with keys as topic #'s and array of that 
        topic's representative docs as values
    """

    all_topics = bert_model.get_topics()
    all_topics_rep = {}

    for key in all_topics.keys():
        if (key == -1):
            continue

        all_topics_rep[key] = bert_model.get_representative_docs(key)

    return all_topics_rep

def filter_lang(word, not_lang_list):
    _, _, details = cld2.detect(word)

    for _, lang_code, _, _ in details:
        if lang_code in not_lang_list:
            return False

    return True

def filter_unique_english_words(words, n_top=3, not_lang_list=['th', 'ja', 'ar', 'ru', 'zh', 'ko']):
    unique_english_words = []
    seen_words = set()

    for word in words:
        # Check if the word is in English and unique
        if filter_lang(word, not_lang_list=not_lang_list) and word not in seen_words:
            # Check similarity with previously seen words
            is_similar = any(difflib.SequenceMatcher(None, word, seen_word).ratio() > 0.8 for seen_word in seen_words)
            if not is_similar:
                unique_english_words.append(word)
                seen_words.add(word)

                # Break the loop if we have enough words
                if len(unique_english_words) == n_top:
                    break

    return unique_english_words

def process_column(col_value, n_top):
    elem_list = col_value.split("_")
    final_value = " | ".join(filter_unique_english_words(elem_list, n_top=n_top))
    return final_value

# ------------------------------------

def save_visualize_bertopic(topic_model, titles, sentences, reduced_embeddings, folder, n_top=3, representation_model=None, load_in_pickle=False):
    if load_in_pickle:
        # 0. GET TOPICS
        all_topics_rep = get_topic_rep_docs(topic_model)
        df = pd.DataFrame(list(all_topics_rep.items()), columns=['Topic', 'Representative_Docs'])
        df.to_csv(f"{folder}/all_topics_rep.csv", index=False)
        
    # 1. TOPIC INFO
    df = topic_model.get_topic_info()
    df.to_csv(f"{folder}/topic_info.csv", index=False)
    print("1. TOPIC INFO")

    # 2. VISUALIZE TOPICS
    fig = topic_model.visualize_topics(custom_labels=True)
    fig.write_html(f"{folder}/visualize_topics.html")
    print("2. VISUALIZE TOPICS")

    # 3. VISUALIZE HIERARCHY
    fig = topic_model.visualize_hierarchy(custom_labels=True)
    fig.write_html(f"{folder}/visualize_hierarchy.html")
    print("3. VISUALIZE HIERARCHY")

    # 4. VISUALIZE BARCHART
    fig = topic_model.visualize_barchart()
    fig.write_html(f"{folder}/visualize_barchart.html")
    print("4. VISUALIZE BARCHART")

    # 5. VISUALIZE DOCUMENTS ANNOTATIONS
    fig = topic_model.visualize_documents(titles, reduced_embeddings=reduced_embeddings, custom_labels=True)
    fig.write_html(f"{folder}/visualize_documents_annotations.html")
    print("5. VISUALIZE DOCUMENTS ANNOTATIONS")

    # 6. VISUALIZE DOCUMENTS  WITHOUT ANNOTATIONS
    fig = topic_model.visualize_documents(titles, reduced_embeddings=reduced_embeddings, custom_labels=True, hide_annotations=True)
    fig.write_html(f"{folder}/visualize_documents.html")
    print("6. VISUALIZE DOCUMENTS WITHOUT ANNOTATIONS")

    # 7. VISUALIZE HIERARCHY TOPICS
    hierarchical_topics = topic_model.my_hierarchical_topics(sentences, n_words=20, representation_model=representation_model)
    hierarchical_topics['Parent_Name'] = hierarchical_topics['Parent_Name'].apply(lambda x: process_column(x, n_top=n_top))
    hierarchical_topics['Child_Left_Name'] = hierarchical_topics['Child_Left_Name'].apply(lambda x: process_column(x, n_top=n_top))
    hierarchical_topics['Child_Right_Name'] = hierarchical_topics['Child_Right_Name'].apply(lambda x: process_column(x, n_top=n_top))

    hierarchical_topics.to_csv(f"{folder}/hierarchical_topics.csv", index=False)
    fig = topic_model.visualize_hierarchy(hierarchical_topics=hierarchical_topics, custom_labels=True)
    fig.write_html(f"{folder}/visualize_hierarchical_topics.html")
    print("7. VISUALIZE HIERARCHY TOPICS")

    # # 8. VISUALIZE HEATMAP
    # fig = topic_model.visualize_heatmap(custom_labels=True)
    # fig.write_html(f"{folder}/visualize_heatmap.html")
    # print("8. VISUALIZE HEATMAP")

    # # 9. VISUALIZE HIERARCHICAL DOCUMENTS
    # fig = topic_model.visualize_hierarchical_documents(sentences, hierarchical_topics, reduced_embeddings=reduced_embeddings, hide_annotations=True)
    # fig.write_html(f"{folder}/visualize_hierarchical_documents.html")
    # print("9. VISUALIZE HIERARCHICAL DOCUMENTS")