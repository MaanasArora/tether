from dataclasses import dataclass
from typing import Iterator
from tether.dataset.source import DataSource, Package, Dataset, Column
from tqdm import tqdm


@dataclass
class DataRepository:
    data_source: DataSource
    packages: dict[str, Package] = None
    datasets: dict[str, Dataset] = None
    columns: dict[str, dict[str, Column]] = None

    def load_all_metadata(self, max_datasets=None) -> None:
        """
        Load all metadata from the data source.
        """
        if self.packages is None:
            self.packages = {}
        if self.datasets is None:
            self.datasets = {}
        if self.columns is None:
            self.columns = {}

        package_names = self.data_source.get_package_names()

        for name in tqdm(package_names, desc="Loading packages"):
            package = Package(name=name, data_source=self.data_source)
            self.packages[name] = package

            dataset_ids = package.get_dataset_ids()
            for dataset_id in dataset_ids:
                dataset = Dataset(id=dataset_id, package=package)

                if dataset.get_path().exists():
                    self.datasets[dataset_id] = dataset

                    columns = dataset.get_columns()
                    for column in columns:
                        col = Column(name=column, dataset=dataset)
                        self.columns[dataset_id] = self.columns.get(dataset_id, {})
                        self.columns[dataset_id][column] = col

                if max_datasets is not None and len(self.datasets) >= max_datasets:
                    return

    def list_packages(self) -> Iterator[Package]:
        """
        List all packages in the repository.
        """
        for package in self.packages.values():
            yield package

    def get_package(self, name: str) -> Package:
        """
        Get a package by name.
        """
        return self.packages.get(name)
    
    def list_datasets(self) -> Iterator[Dataset]:
        """
        List all datasets in the repository.
        """
        for dataset in self.datasets.values():
            yield dataset

    def list_package_datasets(self, package_name: str) -> Iterator[Dataset]:
        """
        List all datasets in a package.
        """
        package = self.get_package(package_name)
        if package:
            for dataset_id in package.get_dataset_ids():
                if dataset_id in self.datasets:
                    yield self.datasets[dataset_id]

    def get_dataset(self, dataset_id: str) -> Dataset:
        """
        Get a dataset by its ID.
        """
        return self.datasets.get(dataset_id)

    def get_column(self, dataset_id: str, column_name: str) -> Column:
        """
        Get a column by dataset ID and column name.
        """
        return self.columns.get(dataset_id, {}).get(column_name)
