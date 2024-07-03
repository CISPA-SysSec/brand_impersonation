import umap

from projection import ProjectionAlgorithm
import numpy as np


class ProjectUMAP(ProjectionAlgorithm):

    def __init__(self, n_neighbors=15, min_dist=0.1, n_components=2, metric="euclidean", random_state=None, **kwargs):

        super().__init__()

        self.n_neighbors = n_neighbors
        self.min_dist = min_dist
        self.n_components = n_components
        self.metric = metric
        if random_state is not None:
            self.random_state = random_state
        else:
            self.random_state = 42
        self.kwargs = kwargs

        self.umap_obj = umap.UMAP(n_neighbors=self.n_neighbors,
                                  min_dist=self.min_dist,
                                  n_components=self.n_components,
                                  metric=self.metric,
                                  random_state=self.random_state, **self.kwargs)

    def get_runner(self):
        return self.umap_obj

    def forward(self, X: np.array) -> np.array:
        print("Emb metric: ", self.metric)
        X_emb = self.umap_obj.fit_transform(X)

        return X_emb
