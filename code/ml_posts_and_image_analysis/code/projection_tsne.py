from sklearn.manifold import TSNE

from projection import ProjectionAlgorithm
import numpy as np


class ProjectTSNE(ProjectionAlgorithm):

    def __init__(self, n_components: int = 2, metric="euclidean", **kwargs):

        super().__init__()

        self.n_components = n_components
        self.metric = metric
        self.kwargs = kwargs

        self.tsne = TSNE(self.n_components, metric=self.metric, **self.kwargs)
    
    def get_runner(self):
        return self.tsne
    
    def forward(self, X: np.array) -> np.array:
        print("Emb metric: ", self.metric)
        X_emb = self.tsne.fit_transform(X)

        return X_emb