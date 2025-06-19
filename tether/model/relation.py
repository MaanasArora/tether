import numpy as np
import pandas as pd
from tqdm import tqdm
from tether.dataset.source import Column
from tether.model.cluster import Domain


def ppmi(
    matrix: np.ndarray,
    smoothing: float = 1e-8,
) -> np.ndarray:
    """
    Calculate Positive Pointwise Mutual Information (PPMI) between two arrays.
    Args:
        x (np.ndarray): First array of counts.
        y (np.ndarray): Second array of counts.
        min_count (int): Minimum count threshold to consider.
        smoothing (float): Smoothing factor to avoid division by zero.
    Returns:
        np.ndarray: PPMI matrix.
    """
    co_matrix = matrix.T @ matrix
    total = np.sum(co_matrix)
    row_sums = np.sum(co_matrix, axis=1, keepdims=True)

    prob = co_matrix
    expected = row_sums @ row_sums.T / total

    pmi = np.log((prob + smoothing) / (expected + smoothing))
    ppmi = np.clip(pmi, a_min=0, a_max=None)

    return ppmi


def get_domain_relations(domains: list[Domain], dataset_ids: list[str]) -> pd.DataFrame:
    dataset_domain_matrix = np.zeros((len(dataset_ids), len(domains)), dtype=int)

    for domain_index, domain in enumerate(tqdm(domains, desc="Processing domains")):
        for column in domain.columns:
            dataset_index = dataset_ids.index(column.dataset.id)
            dataset_domain_matrix[dataset_index, domain_index] += 1

    correlations = ppmi(dataset_domain_matrix, smoothing=1e-8)

    pd_matrix = pd.DataFrame(
        correlations,
        index=[domain.name for domain in domains],
        columns=[domain.name for domain in domains],
    )

    return pd_matrix


def get_column_id(column: Column) -> str:
    return f"{column.dataset.package.name}.{column.name}"


def get_column_relations(
    domain_relations: pd.DataFrame,
    columns: list[Column],
    max_relations: int = 10,
) -> pd.DataFrame:
    column_names = [get_column_id(col) for col in columns]
    column_relations = pd.DataFrame(
        index=column_names,
        columns=column_names,
        dtype=float,
    )

    for i, col1 in enumerate(tqdm(columns, desc="Processing columns")):
        for j, col2 in enumerate(columns):
            if i == j:
                continue
            if col1.type and col2.type and col1.type != col2.type:
                relation_score = domain_relations.loc[col1.type, col2.type]
                column_relations.at[get_column_id(col1), get_column_id(col2)] = (
                    relation_score
                )

    for col in tqdm(columns, desc="Selecting top relations"):
        col_id = get_column_id(col)
        top_relations = column_relations[col_id].nlargest(max_relations)
        top_relations = top_relations[top_relations.index != col_id]
        top_relations = top_relations[top_relations > 0.5]
        column_relations[col_id] = 0
        column_relations.loc[top_relations.index, col_id] = top_relations.values

    column_relations.fillna(0, inplace=True)
    column_relations = column_relations.astype(float)
    return column_relations
