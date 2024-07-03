from projection import ProjectionAlgorithm
import numpy as np


class ProjectIdentity(ProjectionAlgorithm):

    def __init__(self, **kwargs):
        super().__init__()

    def get_runner(self):
        return None

    def forward(self, X: np.array) -> np.array:
        return X
