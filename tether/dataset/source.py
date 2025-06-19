from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
import json
import pandas as pd


@dataclass
class DataSource:
    data_dir: Path
    package_dir: str
    resource_dir: str

    def get_package_names(self) -> list[str]:
        """
        Get a list of package names available in the data source.
        """
        package_path = self.data_dir / self.package_dir
        if not package_path.exists():
            raise FileNotFoundError(f"Package directory {package_path} does not exist.")
        return [f.stem for f in package_path.glob("*.json") if f.is_file()]


class DataElement:
    data_source: DataSource


class DataFile(DataElement):
    def get_path(self) -> Path:
        raise NotImplementedError("Subclasses must implement get_path method")

    def load(self):
        raise NotImplementedError("Subclasses must implement load method")


@dataclass
class Package(DataFile):
    data_source: DataSource
    name: str

    def get_path(self) -> Path:
        return (
            self.data_source.data_dir
            / self.data_source.package_dir
            / f"{self.name}.json"
        )

    def load(self) -> dict:
        path = self.get_path()
        if not path.exists():
            raise FileNotFoundError(f"Package file {path} does not exist.")
        with open(path, "r") as f:
            return json.load(f)

    def get_dataset_ids(self) -> Iterator[str]:
        """
        Get a list of dataset IDs contained in this package.
        """
        package_data = self.load()

        for resource in package_data["result"].get("resources", []):
            yield resource.get("id")


@dataclass
class Dataset(DataFile):
    package: Package
    id: str
    name: str = None

    def __post_init__(self):
        if self.name is None:
            self.name = self.id

        self.data_source = self.package.data_source

    def get_path(self) -> Path:
        return (
            self.data_source.data_dir / self.data_source.resource_dir / f"{self.id}.csv"
        )

    def load(self, nrows=None) -> pd.DataFrame:
        path = self.get_path()
        if not path.exists():
            raise FileNotFoundError(f"Dataset file {path} does not exist.")
        return pd.read_csv(path, low_memory=False, nrows=nrows)

    def get_columns(self) -> list[str]:
        """
        Get a list of column names in the dataset.
        """
        dataframe = self.load()
        return list(dataframe.columns) if not dataframe.empty else []


@dataclass
class Column(DataElement):
    dataset: Dataset
    name: str
    type: str = None

    def get(self, dataframe: pd.DataFrame) -> pd.Series:
        if self.name not in dataframe.columns:
            raise KeyError(f"Column {self.name} does not exist in the dataset.")
        return dataframe[self.name]
