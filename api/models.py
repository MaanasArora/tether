from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from api.db import Base


class Domain(Base):
    __tablename__ = "domains"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    columns = relationship("DatasetColumn", back_populates="domain")


class Package(Base):
    __tablename__ = "packages"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    datasets = relationship("Dataset", back_populates="package")


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    package_id = Column(Integer, ForeignKey("packages.id"), nullable=False)

    package = relationship("Package", back_populates="datasets")
    columns = relationship("DatasetColumn", back_populates="dataset")


class DatasetColumn(Base):
    __tablename__ = "columns"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    domain_id = Column(Integer, ForeignKey("domains.id"), nullable=True)

    dataset = relationship("Dataset", back_populates="columns")
    domain = relationship("Domain", back_populates="columns")
    examples = relationship("Example", back_populates="column")


class Example(Base):
    __tablename__ = "examples"

    id = Column(Integer, primary_key=True)
    value = Column(String, nullable=False)
    column_id = Column(Integer, ForeignKey("columns.id"), nullable=False)

    column = relationship("DatasetColumn", back_populates="examples")
