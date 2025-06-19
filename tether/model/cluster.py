from dataclasses import dataclass
import numpy as np
import torch
from sklearn.cluster import HDBSCAN
from tqdm import tqdm
from tether.dataset.source import Column
from tether.model.item import ItemAutoencoder, process_ascii


@dataclass
class Domain:
    columns: list[Column]
    name: str = None


@dataclass
class ColumnGaussian:
    mean: np.ndarray
    covariance: np.ndarray


def gaussian_distance(mu1, sigma1, mu2, sigma2):
    sigma = (sigma1 + sigma2) / 2
    diff = mu1 - mu2
    inv_sigma = 1 / sigma

    term1 = 0.125 * np.sum(diff**2 * inv_sigma)

    log_prod_sigma = np.sum(np.log(sigma))
    log_prod_sigma1 = np.sum(np.log(sigma1))
    log_prod_sigma2 = np.sum(np.log(sigma2))

    term2 = 0.5 * (log_prod_sigma - 0.5 * (log_prod_sigma1 + log_prod_sigma2))

    return term1 + term2


def encode_column(
    model: ItemAutoencoder, items: list[str], max_items: int = 1000
) -> ColumnGaussian:
    if not items:
        return None

    if len(items) > max_items:
        items = items[:max_items]

    ascii_items = process_ascii(items)
    ascii_items = torch.tensor(ascii_items, dtype=torch.float32)

    encoded = model.encoder(ascii_items)
    mean = encoded.mean(dim=0).detach().cpu().numpy()
    variances = encoded.var(dim=0).detach().cpu().numpy()
    variances = np.nan_to_num(variances, nan=1e-4)
    variances = np.maximum(variances, 1e-4)

    return ColumnGaussian(mean=mean, covariance=variances)


def cluster_columns(
    model: ItemAutoencoder,
    items: list[list[str]],
    columns: list[Column],
    min_cluster_size: int = 3,
) -> list[Domain]:
    if not items or not columns:
        return []

    gaussians = []
    for i, column in enumerate(tqdm(columns, desc="Encoding columns")):
        if not items[i]:
            continue

        gaussian = encode_column(model, items[i])
        if gaussian is not None:
            gaussians.append((gaussian, column))

    distances = np.zeros((len(gaussians), len(gaussians)))
    for i in tqdm(range(len(gaussians)), desc="Computing distances"):
        for j in range(i + 1, len(gaussians)):
            gaussian1, _ = gaussians[i]
            gaussian2, _ = gaussians[j]
            mu1, sigma1 = gaussian1.mean, gaussian1.covariance
            mu2, sigma2 = gaussian2.mean, gaussian2.covariance
            distance = gaussian_distance(mu1, sigma1, mu2, sigma2)
            distances[i, j] = distances[j, i] = distance

    clustering = HDBSCAN(
        metric="precomputed",
        min_cluster_size=min_cluster_size,
    ).fit(distances)

    labels = clustering.labels_
    domains = {}
    for i, label in enumerate(labels):
        if label == -1:
            continue  # Skip noise points
        if label not in domains:
            domains[label] = Domain(columns=[])

        _, column = gaussians[i]
        domains[label].columns.append(column)

    for i, (label, domain) in enumerate(domains.items()):
        col_names = [col.name.upper() for col in domain.columns]
        name_counts = {name: col_names.count(name) for name in set(col_names)}
        most_common_name = max(name_counts, key=name_counts.get)

        if name_counts[most_common_name] > 2:
            domain.name = most_common_name

        for col in domain.columns:
            col.type = i

    return list(domains.values())
