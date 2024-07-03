import matplotlib.pyplot as plt

import os

from utilities import def_decomposition


def cosine_plot(X, yhat, visual_method: str = 'tsne', path_to_save=None):
    embedding_method = def_decomposition(method=visual_method, cosine_sim=True)
    X_plot = embedding_method.forward(X)

    x_point, y_point = X_plot[:, 0], X_plot[:, 1]

    fig, ax = plt.subplots(1, 1, figsize=(10, 5))

    ax.set_title("Cosine Sim Predicted Labels")
    ax.scatter(x_point, y_point, c=yhat, linewidth=0, s=140)
    ax.set_xlim(x_point.min(), x_point.max())
    ax.set_ylim(y_point.min(), y_point.max())
    ax.set_xlabel('Dec. Component 1')
    ax.set_ylabel('Dec. Component 2')

    # Add grid lines
    ax.grid(color='gray', linestyle='dotted', linewidth=0.5, alpha=1.)

    plt.tight_layout()

    # Check if plots_name is None, if True, show the plot, else save it in the specified folder
    if path_to_save is None:
        plt.show()
        plt.waitforbuttonpress()
    else:
        if not os.path.exists(path_to_save):
            os.makedirs(path_to_save)

        plt.savefig(f'{path_to_save}/cosine_clusters.png')

    plt.close()


def standard_plot(X, yhat, visual_method: str = 'pca', path_to_save=None):
    if X.shape[1] >= 2:
        embedding_method = def_decomposition(method=visual_method)
        X_plot = embedding_method.forward(X)
    else:
        X_plot = X.squeeze(2)

    x_point, y_point = X_plot[:, 0], X_plot[:, 1]

    fig, ax = plt.subplots(1, 1, figsize=(10, 5))

    ax.set_title("Standard Predicted Labels")
    ax.scatter(x_point, y_point, c=yhat, linewidth=0, s=140)
    ax.set_xlim(x_point.min(), x_point.max())
    ax.set_ylim(y_point.min(), y_point.max())
    ax.set_xlabel('Dec. Component 1')
    ax.set_ylabel('Dec. Component 2')

    # Add grid lines
    ax.grid(color='gray', linestyle='dotted', linewidth=0.5, alpha=1.)

    plt.tight_layout()

    # Check if plots_name is None, if True, show the plot, else save it in the specified folder
    if path_to_save is None:
        plt.show()
        plt.waitforbuttonpress()
    else:
        if not os.path.exists(path_to_save):
            os.makedirs(path_to_save)

        plt.savefig(f'{path_to_save}/standard_clusters.png')

    plt.close()
