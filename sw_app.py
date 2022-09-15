from dotenv import dotenv_values
import asyncio
import datetime
import aiohttp
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from more_itertools import chunked
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

env = dotenv_values(".env")
Base = declarative_base()
CHUNK_SIZE = 10
PG_DSN = f'postgresql+asyncpg://{env["DB_USER"]}:{env["DB_PASSWORD"]}@{env["DB_HOST"]}:{env["DB_PORT"]}/{env["DB_NAME"]}'
engine = create_async_engine(PG_DSN)
column_list = ['birth_year', 'eye_color', 'films', 'gender', 'hair_color', 'height', 'homeworld', 'mass', 'name', 'skin_color', 'species' , 'starships', 'vehicles']


class People(Base):

    __tablename__ = 'people'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    eye_color = Column(String, nullable=False)
    films = Column(String, nullable=False)
    gender = Column(String, nullable=False)
    hair_color = Column(String, nullable=False)
    height = Column(String, nullable=False)
    homeworld = Column(String, nullable=False)
    mass = Column(String, nullable=False)
    skin_color = Column(String, nullable=False)
    species = Column(String, nullable=False)
    starships = Column(String, nullable=False)
    vehicles = Column(String, nullable=False)
    birth_year = Column(String, nullable=False)


async def get_people(session, people_id):
    result = await session.get(f'https://swapi.dev/api/people/{people_id}')
    return await result.json()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with aiohttp.ClientSession() as web_session:

        people_list = []
        for chunk_id in chunked(range(1, 21), CHUNK_SIZE):
            coros = [get_people(web_session, i) for i in chunk_id]
            result = await asyncio.gather(*coros)
            for people in result:
                people_dict = {}
                for key, value in people.items():
                    if key in column_list:
                        if type(value) == list:
                            people_dict[key] = ', '.join(value)
                        else:
                            people_dict[key] = value
                if len(people_dict) != 0:
                    people_list.append(people_dict)
                print(people)
        peoples = [People(**people) for people in people_list]
        async with async_session_maker() as orm_session:
            orm_session.add_all(peoples)
            await orm_session.commit()


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)


