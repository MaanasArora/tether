import argparse
from pathlib import Path

import pandas as pd
from tether.dataset.source import DataSource, Column
from tether.dataset.repository import DataRepository
from tether.model.item import ItemAutoencoder, load_model
from tether.model.cluster import cluster_columns
from tqdm import tqdm

from tether.model.relation import get_domain_relations
from tether.utils.database import make_metadata_for_db, save_metadata_to_db


def main():
    parser = argparse.ArgumentParser(
        description="Domain Clustering and Knowledge Graph Creation"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Directory containing the dataset files",
    )
    parser.add_argument(
        "--package-dir",
        type=str,
        default="packages",
        help="Directory containing the package metadata",
    )
    parser.add_argument(
        "--resource-dir",
        type=str,
        default="resources",
        help="Directory containing additional resources",
    )
    parser.add_argument(
        "--max-datasets",
        type=int,
        default=None,
        help="Maximum number of datasets to process",
    )
    parser.add_argument(
        "--model-path",
        type=str,
        default="tether/checkpoints/item_autoencoder.pth",
        help="Path to the pre-trained model checkpoint",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    package_dir = Path(args.package_dir)
    resource_dir = Path(args.resource_dir)
    max_datasets = args.max_datasets
    model_path = Path(args.model_path)

    data_source = DataSource(
        data_dir=data_dir, package_dir=package_dir, resource_dir=resource_dir
    )

    data_repository = DataRepository(data_source=data_source)
    data_repository.load_all_metadata(max_datasets=max_datasets)

    model = ItemAutoencoder(input_dim=256, hidden_dim=64, input_size=100)
    if model_path.exists():
        model = load_model(model, model_path)
    else:
        print(
            f"Model checkpoint not found at {model_path}. Please train the model first."
        )
        return

    datasets = list(data_repository.list_datasets())

    items = []
    columns = []
    for dataset in tqdm(datasets, desc="Processing datasets"):
        df = dataset.load()
        for column in df.columns:
            if df[column].dtype != "object":
                continue
            if df[column].isnull().all():
                continue
            col = Column(name=column, dataset=dataset)
            columns.append(col)
            items.append(df[column].dropna().tolist())

    domains = cluster_columns(
        model=model,
        items=items,
        columns=columns,
        min_cluster_size=3,
    )

    metadata_db = make_metadata_for_db(
        packages=list(data_repository.list_packages()),
        datasets=list(data_repository.list_datasets()),
        domains=domains,
        columns=columns,
    )

    save_metadata_to_db(*metadata_db)
    print("Metadata saved to database.")

    domain_relations = get_domain_relations(
        domains=domains,
        dataset_ids=[dataset.id for dataset in datasets],
    )
    domain_relations_df = pd.DataFrame(domain_relations)

    index = metadata_db[2].id
    domain_relations_df.index = index
    domain_relations_df.columns = index
    
    domain_relations_df.fillna(0, inplace=True)
    domain_relations_df = domain_relations_df.astype(float)
    domain_relations_df = domain_relations_df.round(2)

    domain_relations_df.to_csv("data/output/domain_relations.csv")
    print("Domain relations saved to data/output/domain_relations.csv.")


if __name__ == "__main__":
    main()
