from sklearn.cluster import AgglomerativeClustering

from clustering import ClusteringAlgorithm

from pairwise_distances import preprocessing_embeddings

NOT_IMPLEMENED_METRICS_AGGLOMERATIVE = ['dot']

class ClusteringAgglomerative(ClusteringAlgorithm):

    def __init__(self, clustering_metric: str, linkage: str = 'average', distance_threshold: float = 0.4, n_clusters: int=None, device=None, **kwargs):

        super().__init__("agglomerative", clustering_metric, device)
        
        self.n_clusters = n_clusters
        self.linkage = linkage
        self.distance_threshold = distance_threshold
        self.kwargs = kwargs

        if(self.clustering_metric in NOT_IMPLEMENED_METRICS_AGGLOMERATIVE):
            self.runner_metric = "precomputed"
        else:
            self.runner_metric = self.clustering_metric

        self.runner = AgglomerativeClustering(n_clusters=self.n_clusters, metric=self.runner_metric, distance_threshold=self.distance_threshold, linkage=self.linkage, **self.kwargs)

    def get_runner(self):
        return self.runner

    # capire se devi rifare reset...
    def forward(self, X):
        print("Clu runner metric: ", self.runner_metric)
        if self.runner_metric == "precomputed":
            X = preprocessing_embeddings(X, clustering_metric=self.clustering_metric)
        self.runner.fit(X)
        return self.runner.labels_

    def reset(self):
        self.runner = AgglomerativeClustering(n_clusters=self.n_clusters, metric=self.runner_metric, distance_threshold=self.distance_threshold, linkage=self.linkage, **self.kwargs)