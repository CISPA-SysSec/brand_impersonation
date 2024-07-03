from sklearn.cluster import KMeans

from clustering import ClusteringAlgorithm

from pairwise_distances import preprocessing_embeddings

NOT_IMPLEMENED_METRICS_KMEANS = []


class ClusteringKmeans(ClusteringAlgorithm):

    def __init__(self, clustering_metric: str, n_clusters: int, init: str = "k-means++", device=None, **kwargs):

        super().__init__("kmeans", clustering_metric, device)

        self.n_clusters = n_clusters
        self.init = init
        self.kwargs = kwargs

        if self.clustering_metric in NOT_IMPLEMENED_METRICS_KMEANS:
            self.runner_metric = "precomputed"
        else:
            self.runner_metric = self.clustering_metric

        self.runner = KMeans(n_clusters=self.n_clusters, init=self.init, **self.kwargs)

    def get_runner(self):
        return self.runner

    def forward(self, X):
        print("Clu runner metric: ", self.runner_metric)
        if self.runner_metric == "precomputed":
            X = preprocessing_embeddings(X, clustering_metric=self.clustering_metric)
        return self.runner.fit_predict(X)

    def reset(self):
        self.runner = KMeans(n_clusters=self.n_clusters, init=self.init, **self.kwargs)
