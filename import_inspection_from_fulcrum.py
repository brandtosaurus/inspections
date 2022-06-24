import pandas as pd
import numpy as np

# import requests
# from urllib3 import request
# import json
import sqlalchemy as sa

# import psycopg2

from io import StringIO
import csv

from win10toast import ToastNotifier

import road_index_calculations as calc

toast = ToastNotifier()
toast.show_toast(
    "SCRIPT RUNNING",
    "Inserting records from FulcrumApp",
    duration=10,
)

TABLE = "road_visual_assessment_view"
CREATED_TABLE = "road_visual_assessment_created"

SCHEMA = "assessment"

CSV = r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.csv"
CSV_ANCILLARY = (
    r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.csv?child=ancillary_assets"
)

JSON = r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.json"

GEOJSON = r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.geojson"
GEOJSON_ANCILLARY = (
    r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.geojson?child=ancillary_assets"
)

DB_NAME = "wc_asset_management"
DB_USER = "postgres"
DB_PASS = "post@dmin100!"
DB_HOST = "10.73.1.2"
DB_PORT = "5436"

ENGINE_URL = sa.engine.URL.create(
    "postgresql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

ENGINE = sa.create_engine(
    ENGINE_URL
)

PROJECT = 'ODM RRAMS 2021 - 2023'

def get_int_columns():
    cols_qry = """select column_name
    from information_schema.columns
    where table_schema = 'assessment' and table_name = 'road_visual_assessment' 
    and data_type in ('integer', 'smallint', 'bigint');"""
    cols = pd.read_sql_query(cols_qry, ENGINE)
    cols = list(cols['column_name'])
    return cols

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def main():
    try:

        df = pd.read_csv(CSV, low_memory=False)
        ancillary_data = pd.read_csv(CSV_ANCILLARY, low_memory=False)

        try:
            df.rename(columns = {'project':'project_name', 'kerbs': 'kerbs_degree'}, inplace = True)
        except:
            pass
        df['kerbs_degree'].loc[df['kerbs_degree']=='N'] = np.nan

        cols = pd.read_sql_query("select * from assessment.road_visual_assessment limit 1;", ENGINE)
        cols = list(cols.columns)

        int_cols = get_int_columns()
        int_cols = [i for i in int_cols if i in df.columns]

        inspected = df[df["status"] == "inspected"]
        inspected["segment_id"] = inspected["asset_id"]
       
        # inspected = calc.main(inspected)

        created = df[df["status"] == "created"]
        created["segment_id"] = created["fulcrum_id"]

        # created = calc.main(created)

        inspected.drop('asset_id', axis=1, inplace=True)

        inspected[int_cols] = inspected[int_cols].astype(int).fillna(0)
        created[int_cols] = created[int_cols].astype(int).fillna(0)

        inspected = inspected[inspected.columns.intersection(cols)]
        created = created[created.columns.intersection(cols)]

        inspected.to_sql(
            TABLE,
            ENGINE,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )
        created.to_sql(
            TABLE,
            ENGINE,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )

    # try:
    #     conn = psycopg2.connect(
    #         dbname="asset_management_master",
    #         user="postgres",
    #         password="$admin",
    #         host="localhost",
    #         port=5432,
    #     )
    #     conn.set_session(autocommit=True)
    #     cur = conn.cursor()
    #     cur.callproc(
    #         "assessment.rva_indices",
    #     )
    # except (Exception, psycopg2.DatabaseError) as e:
    #     print(e)
    # finally:
    #     if conn is not None:
    #         conn.close()

    # try:
    #     conn = psycopg2.connect(
    #         dbname="asset_management_master",
    #         user="postgres",
    #         password="$admin",
    #         host="localhost",
    #         port=5432,
    #     )
    #     conn.set_session(autocommit=True)
    #     cur = conn.cursor()
    #     cur.callproc(
    #         "assessment.rva_indices_2",
    #     )
    # except (Exception, psycopg2.DatabaseError) as e:
    #     print(e)
    # finally:
    #     if conn is not None:
    #         conn.close()

        toast.show_toast(
            "SCRIPT RAN SUCCESSFULLY",
            "Inserting records from FulcrumApp",
            duration=10,
        )
    except:
        toast.show_toast(
            "SOMETHING WENT WRONG - PLEASE CHECK IMPORT FUNCTION",
            "Inserting records from FulcrumApp",
            duration=10,
        )


if __name__ == '__main__':
    main()