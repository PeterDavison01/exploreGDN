from sqlalchemy import create_engine, Engine, MetaData, Table, select, exc
import pandas as pd

def createConnection() -> Engine:
    engine = create_engine(
        f"postgresql://postgres:postgres@localhost:5433/swat-data"
    )

    return engine


def executeStatement(stmt, engine: Engine):
    conn = engine.connect()

    try:
        ret = list()
        for row in conn.execute(stmt):
            ret.append(row._asdict())
        conn.close()
        return ret
    except exc.DBAPIError as e:
        print(f"RAW_ERROR: {e}")
        if e.connection_invalidated:
            print("ERROR: Connection Invalidated!!")


def fetchData() -> pd.DataFrame:
    engine = createConnection()
    metadata = MetaData()

    data_table = Table("SWaT", metadata, schema="public", autoload_with=engine)

    stmt = select(data_table)

    data = pd.DataFrame(executeStatement(stmt, engine), columns=data_table.columns.keys())

    return data

