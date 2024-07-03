from sentence_transformers import util

from clustering import ClusteringAlgorithm

import numpy as np


class ClusteringFastClustering(ClusteringAlgorithm):

    def __init__(self, clustering_metric: str, min_community_size: int, threshold: int, device, **kwargs):

        if clustering_metric != 'cosine':
            raise 'FastClustering compatible only with cosine metric'

        super().__init__("fast_clustering", clustering_metric, device)

        self.min_community_size = min_community_size
        self.threshold = threshold
        self.kwargs = kwargs

        self.runner_metric = 'cosine'

    def __clusters_to_labels(self, dim, clusters):
        yhat_fast = np.full(dim, -1)

        for i, cluster in enumerate(clusters):
            for idx_elem in cluster:
                yhat_fast[idx_elem] = i

        return yhat_fast

    def get_runner(self):
        assert "Clustering obj not exist in this method."

    def forward(self, X):
        print("Clu runner metric: ", self.runner_metric)
        X = X.to(self.device)
        clusters = util.community_detection(X, min_community_size=self.min_community_size, threshold=self.threshold,
                                            **self.kwargs)
        yhat = self.__clusters_to_labels(X.shape[0], clusters)

        return yhat
