import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func


def get_null_count(conn_str, table):
    engine = sa.create_engine('clickhouse://default@10.0.0.2:8123/default')

    metadata = sa.MetaData(bind=engine)
    metadata.reflect(only=[table])
    tbl = metadata.tables[table]

    Session = sessionmaker(bind=engine)
    session = Session()

    col_names = [col.name for col in tbl.columns]
    null_sums = [
        func.sum(tbl.c[name].is_(None)) for name in col_names]

    qry = session.query(*null_sums)
    return dict(zip(col_names, qry.first()))
