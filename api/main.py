from typing import Annotated
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
import api.models as models
from api.db import engine, Base, get_db


# Ensure the database tables are created
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development; adjust in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    await init_db()


class Package(BaseModel):
    id: int
    name: str


class Dataset(BaseModel):
    id: int
    name: str
    package: Package


class Example(BaseModel):
    value: str


class Column(BaseModel):
    id: int
    name: str
    dataset: Dataset
    examples: list[Example] = []


class Domain(BaseModel):
    id: int
    name: str | None = None


class DomainWithColumns(Domain):
    columns: list[Column] = []


class DomainRelation(BaseModel):
    source: str
    target: str
    weight: float


class DomainRelationsResponse(BaseModel):
    nodes: list[DomainWithColumns]
    edges: list[DomainRelation]


@app.get("/")
async def read_root():
    return {"message": "Welcome to the Tether API!"}


@app.get("/packages", response_model=list[Package])
async def get_packages(db: Annotated[AsyncSession, Depends(get_db)]):
    packages = await db.execute(select(models.Package).order_by(models.Package.name))
    packages = packages.scalars().all()
    return packages


@app.get("/domains", response_model=list[Domain])
async def get_domains(db: Annotated[AsyncSession, Depends(get_db)]):
    domains = await db.execute(select(models.Domain).order_by(models.Domain.name))
    domains = domains.scalars().all()
    return domains


@app.get("/domains/{domain_id}", response_model=DomainWithColumns)
async def get_domain(domain_id: int, db: Annotated[AsyncSession, Depends(get_db)]):
    domain = await db.execute(
        select(models.Domain)
        .where(models.Domain.id == domain_id)
        .options(
            selectinload(models.Domain.columns)
            .selectinload(models.DatasetColumn.dataset)
            .selectinload(models.Dataset.package),
            selectinload(models.Domain.columns).selectinload(
                models.DatasetColumn.examples
            ),
        )
    )
    domain = domain.scalar_one_or_none()
    if not domain:
        return {"error": "Domain not found"}, 404
    return domain


@app.get("/domain-relations")
async def get_domain_relations(
    db: Annotated[AsyncSession, Depends(get_db)],
    nlargest: int = 10,
    min_weight: float = 0.5,
    num_examples: int = 10,
):
    matrix_df = pd.read_csv("../data/output/domain_relations.csv", index_col=0)
    matrix_df.fillna(0, inplace=True)

    domains = await db.execute(
        select(models.Domain)
        .where(models.Domain.id.in_(matrix_df.index))
        .options(
            selectinload(models.Domain.columns)
            .selectinload(models.DatasetColumn.dataset)
            .selectinload(models.Dataset.package),
            selectinload(models.Domain.columns).selectinload(
                models.DatasetColumn.examples
            ),
        )
        .order_by(models.Domain.name)
    )

    domains = domains.scalars().all()
    for domain in domains:
        for column in domain.columns:
            column.examples = column.examples[:num_examples]

    relations = {}
    for domain_id in matrix_df.index:
        if domain_id not in relations:
            relations[domain_id] = []

        sorted_relations = matrix_df.loc[domain_id].nlargest(nlargest)
        for related_domain_id, score in sorted_relations.items():
            if related_domain_id != domain_id:
                relations[domain_id].append(
                    {
                        "related_domain_id": related_domain_id,
                        "score": score,
                    }
                )

    edges = []
    for domain_id, related_domains in relations.items():
        for relation in related_domains:
            if relation["score"] < min_weight:
                continue
            edges.append(
                {
                    "source": str(domain_id),
                    "target": str(relation["related_domain_id"]),
                    "weight": relation["score"],
                }
            )

    return {"nodes": domains, "edges": edges}
