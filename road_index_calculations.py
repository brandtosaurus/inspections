import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
import pandasql

ENGINE = create_engine(
    "postgresql://postgres:$admin@localhost:5432/asset_management_master"
)

QRY = """SELECT * FROM assessment.road_visual_assessment rva WHERE rva.visual_condition_index_vci IS NULL 
		AND rva.visual_gravel_index_vgi IS NULL AND rva.structural_condition_index_stci IS null"""
ASSETS_QRY = """SELECT * FROM infrastructure.asset where asset_type_id = 2"""
RISFSA_QRY = """SELECT * FROM lookups.risfsa"""
RAINFALL_QRY = """SELECT * FROM base_layers.mean_rainfall"""


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


def calculate_indices(df):

    return df


def calculate_mni(df):
    assets = gpd.GeoDataFrame.from_postgis(ASSETS_QRY, ENGINE, geom_col="geom")
    risfsa = pd.read_sql_query(RISFSA_QRY, ENGINE)
    rainfall = gpd.GeoDataFrame.from_postgis(RAINFALL_QRY, ENGINE, geom_col="geom")
    mni_weights = pd.read_sql_table("mni_weights", ENGINE, schema="lookups")

    adf1 = rainfall[["sde_sde_1", "rainfall_id"]]
    adf2 = risfsa[["class", "risfsa_id"]]
    assets = assets.merge(adf1, on="rainfall_id")
    assets = assets.merge(adf2, on="risfsa_id")
    assets = assets[["asset_id", "sde_sde_1", "class"]]
    df = df.merge(assets, on="asset_id")

    df["class"] = df["class"].fillna("Class 5")
    df["road_category_type"] = df["road_category_type"].fillna(
        "Paved Single Carriageway"
    )

    df["sde_sde_1"] = df.loc[df["sde_sde_1"] == 1, "sde_sde_1"] = "Dry"
    df["sde_sde_1"] = df.loc[df["sde_sde_1"] == 2, "sde_sde_1"] = "Dry"
    df["sde_sde_1"] = df.loc[df["sde_sde_1"] == 3, "sde_sde_1"] = "Moderate"
    df["sde_sde_1"] = df.loc[df["sde_sde_1"] == 4, "sde_sde_1"] = "Moderate"
    df["sde_sde_1"] = df.loc[df["sde_sde_1"] == 5, "sde_sde_1"] = "Wet"
    df["sde_sde_1"] = df.loc[df["sde_sde_1"] == 6, "sde_sde_1"] = "Wet"

    df["sub_category"] = df.loc[
        (df["visual_condition_index_vci"] <= 30), "sde_sde_1"
    ] = "Fair"
    df["sub_category"] = df.loc[
        (df["visual_condition_index_vci"] > 30)
        & (df["visual_condition_index_vci"] <= 50),
        "sde_sde_1",
    ] = "Good"
    df["sub_category"] = df.loc[
        (df["visual_condition_index_vci"] > 50)
        & (df["visual_condition_index_vci"] <= 70),
        "sde_sde_1",
    ] = "Very Good"
    df["sub_category"] = df.loc[
        (df["visual_condition_index_vci"] > 70)
        & (df["visual_condition_index_vci"] <= 85),
        "sde_sde_1",
    ] = "Poor"
    df["sub_category"] = df.loc[
        (df["visual_condition_index_vci"] > 85), "sde_sde_1"
    ] = "Very Poor"

    mni_weights["importance_join"] = (
        mni_weights["sub_category"] + mni_weights["road_type"]
    )
    mni_weights["rainfall_join"] = (
        mni_weights["sub_category"] + mni_weights["road_type"]
    )
    mni_weights["social_env"] = mni_weights["sub_category"] + mni_weights["road_type"]

    df = pd.merge(
        df, mni_weights[["importance_join", "weight"]], on="importance_join", how="left"
    )
    df.dropna(axis=1, how="all", inplace=True)
    df.rename(columns={"weight": "importance_weight"}, inplace=True)
    df["importance_weight"] = df["importance_weight"].fillna(0)

    df = pd.merge(
        df, mni_weights[["rainfall_join", "weight"]], on="rainfall_join", how="left"
    )
    df.dropna(axis=1, how="all", inplace=True)
    df.rename(columns={"weight": "rainfall_weight"}, inplace=True)
    df["rainfall_weight"] = df["rainfall_weight"].fillna(0)

    df = pd.merge(
        df, mni_weights[["social_env", "weight"]], on="social_env", how="left"
    )
    df.dropna(axis=1, how="all", inplace=True)
    df.rename(columns={"weight": "social_weight"}, inplace=True)
    df["social_weight"] = df["social_weight"].fillna(0)

    df = pd.merge(
        df, mni_weights[["sub_category", "weight"]], on="sub_category", how="left"
    )
    df.dropna(axis=1, how="all", inplace=True)
    df.rename(columns={"weight": "cond_weight"}, inplace=True)
    df["cond_weight"] = df["cond_weight"].fillna(0)

    df["maintenance_need_index_mni"] = round(
        (
            (
                df["importance_weight"]
                + df["rainfall_weight"]
                + df["social_weight"]
                + df["cond_weight"]
            )
            / 6.1
        )
        * 100,
        3,
    )

    df = df[["visual_assessment_id", "maintenance_need_index_mni"]]

    return df


def main():
    df = gpd.GeoDataFrame.from_postgis(QRY, ENGINE, geom_col="geometry")
    df = calculate_indices(df)

    df = calculate_mni(df)
    pass


if __name__ == "__main__":
    main()
