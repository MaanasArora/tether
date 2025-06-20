import pandas as pd
from sqlalchemy import create_engine, text
from tether.dataset.source import Column, Dataset, Package
from tether.model.cluster import Domain


def make_metadata_for_db(
    packages: list[Package],
    datasets: list[Dataset],
    domains: list[Domain],
    columns: list[Column],
):
    package_to_id = {pkg.name: i for i, pkg in enumerate(packages, 1)}
    dataset_to_id = {ds.id: i for i, ds in enumerate(datasets, 1)}
    column_to_id = {
        f"{col.dataset.id}.{col.name}": i for i, col in enumerate(columns, 1)
    }

    packages_db = pd.DataFrame(
        [{"id": i, "name": pkg.name} for i, pkg in enumerate(packages, 1)],
        columns=["id", "name"],
    )
    datasets_db = pd.DataFrame(
        [
            {
                "id": dataset_to_id[ds.id],
                "name": ds.id,
                "package_id": package_to_id[ds.package.name],
            }
            for ds in datasets
        ],
        columns=["id", "name", "package_id"],
    )
    domains_db = pd.DataFrame(
        [
            {
                "id": i,
                "name": domain.name,
            }
            for i, domain in enumerate(domains, 1)
        ],
        columns=["id", "name"],
    )
    columns_db = pd.DataFrame(
        [
            {
                "id": column_to_id[f"{col.dataset.id}.{col.name}"],
                "name": col.name,
                "dataset_id": dataset_to_id[col.dataset.id],
                "domain_id": col.type + 1 if col.type is not None else None,
            }
            for col in columns
        ],
        columns=["id", "name", "dataset_id", "domain_id"],
    )
    examples_db = pd.DataFrame(
        [
            {
                "id": i * 100 + j,  # Unique ID for each example
                "column_id": column_to_id[f"{col.dataset.id}.{col.name}"],
                "value": example,
            }
            for i, col in enumerate(columns)
            for j, example in enumerate(
                col.dataset.load(nrows=100)[col.name].dropna().unique()
            )
        ],
        columns=["id", "column_id", "value"],
    )
    examples_db["value"] = (
        examples_db["value"].astype(str).str[:50]
    )  # Limit to 50 chars
    examples_db = examples_db.drop_duplicates(subset=["column_id", "value"])
    examples_db = examples_db[examples_db["value"].str.strip() != ""]
    examples_db = examples_db.reset_index(drop=True)

    return (
        packages_db,
        datasets_db,
        domains_db,
        columns_db,
        examples_db,
    )


def save_metadata_to_db(
    packages_db: pd.DataFrame,
    datasets_db: pd.DataFrame,
    domains_db: pd.DataFrame,
    columns_db: pd.DataFrame,
    examples_db: pd.DataFrame,
    db_url: str = "postgresql://postgres:postgres@localhost:5432/tether",
):
    engine = create_engine(db_url)
    with engine.connect() as conn:
        if conn.dialect.has_table(conn, "packages"):
            conn.execute(text("DROP TABLE IF EXISTS examples"))
            conn.execute(text("DROP TABLE IF EXISTS columns"))
            conn.execute(text("DROP TABLE IF EXISTS domains"))
            conn.execute(text("DROP TABLE IF EXISTS datasets"))
            conn.execute(text("DROP TABLE IF EXISTS packages"))

        packages_db.to_sql("packages", conn, if_exists="append", index=False)
        datasets_db.to_sql("datasets", conn, if_exists="append", index=False)
        domains_db.to_sql("domains", conn, if_exists="append", index=False)
        columns_db.to_sql("columns", conn, if_exists="append", index=False)
        examples_db.to_sql("examples", conn, if_exists="append", index=False)
        conn.commit()
