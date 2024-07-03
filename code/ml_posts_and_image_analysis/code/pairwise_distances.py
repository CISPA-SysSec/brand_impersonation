
from sklearn.metrics import pairwise_distances


def pairwise_dot_product(embeddings):
    return 1 - (embeddings @ embeddings.T).numpy()


def preprocessing_embeddings(X, clustering_metric: str):
    if clustering_metric == 'cosine':
        return pairwise_distances(X, metric='cosine')
    elif clustering_metric == 'dot':
        return pairwise_dot_product(X)
    else:
        raise "Preprocessing embedding not implemented"
