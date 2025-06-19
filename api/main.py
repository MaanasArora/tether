from typing import Annotated
import asyncio
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
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
    name: str


class DomainWithColumns(Domain):
    columns: list[Column] = []


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
