from sklearn.decomposition import PCA

from projection import ProjectionAlgorithm
import numpy as np


class ProjectPCA(ProjectionAlgorithm):

    def __init__(self, n_components: int = 2, **kwargs):
        super().__init__()

        self.n_components = n_components
        self.kwargs = kwargs

        self.pca = PCA(self.n_components, **self.kwargs)

    def get_runner(self):
        return self.pca

    def forward(self, X: np.array) -> np.array:
        X_emb = self.pca.fit_transform(X)

        return X_emb
