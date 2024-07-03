from hdbscan import HDBSCAN

from clustering import ClusteringAlgorithm

from pairwise_distances import preprocessing_embeddings

NOT_IMPLEMENED_METRICS_HDBSCAN = ['cosine', 'dot']


class ClusteringHdbscan(ClusteringAlgorithm):

    def __init__(self, clustering_metric: str, min_samples: int, min_cluster_size: int, device=None, **kwargs):

        super().__init__("hdbscan", clustering_metric, device)

        self.min_samples = min_samples
        self.min_cluster_size = min_cluster_size
        self.kwargs = kwargs

        if self.clustering_metric in NOT_IMPLEMENED_METRICS_HDBSCAN:
            self.runner_metric = "precomputed"
        else:
            self.runner_metric = self.clustering_metric

        self.runner = HDBSCAN(min_samples=self.min_samples, min_cluster_size=self.min_cluster_size,
                              metric=self.runner_metric, **self.kwargs)

    def get_runner(self):
        return self.runner

    def forward(self, X):
        print("Clu runner metric: ", self.runner_metric)
        if self.runner_metric == "precomputed":
            X = preprocessing_embeddings(X, clustering_metric=self.clustering_metric).astype("float64")
        return self.runner.fit_predict(X)

    def reset(self):
        self.runner = HDBSCAN(min_samples=self.min_samples, min_cluster_size=self.min_cluster_size,
                              metric=self.runner_metric, **self.kwargs)
