

class ClusteringAlgorithm():

    def __init__(self, name: str, clustering_metric: str, device=None):
        self.name = name
        self.clustering_metric = clustering_metric
        self.device = device

    def get_runner(self):
        pass

    def forward(self, X):
        pass

    def reset(self):
        pass
