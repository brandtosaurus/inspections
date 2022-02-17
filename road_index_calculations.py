from audioop import reverse
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

FCI_BLOCK_LOOKUP_QRY = """SELECT * FROM lookups.fci_block_deduct"""
FCI_CONC_LOOKUP_QRY = """SELECT * FROM lookups.fci_conc_deduct"""
FCI_FLEX_LOOKUP_QRY = """SELECT * FROM lookups.fci_flex_deduct"""
FCI_GRAV_LOOKUP_QRY = """SELECT * FROM lookups.fci_grav_deduct"""

PCI_CONC_LOOKUP_QRY = """SELECT * FROM lookups.pci_conc_deduct"""
PCI_BLOCK_LOOKUP_QRY = """SELECT * FROM lookups.pci_block_deduct"""
PCI_FLEX_LOOKUP_QRY = """SELECT * FROM lookups.pci_flex_deduct"""

SCI_FLEX_LOOKUP_QRY = """SELECT * FROM lookups.sci_flex_deduct"""

SCI_WEIGHTS_QRY = """SELECT * FROM lookups.sci_weights"""
VCI_WEIGHTS_QRY = """SELECT * FROM lookups.vci_weights"""
VGI_DEDUCT_QRY = """SELECT * FROM lookups.vgi_deduct"""


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


# ###########################################
# ###########################################


def stci_deduct_Block_calc(df, df2):
    filter_cols = [col for col in df if col.startswith("b_")]
    filter_cols = filter_cols.append("assessment_id")
    df = df.loc[:, df[filter_cols]]
    df.fillna(0)

    for idx, row in df.iterrows():
        de1 = str(row["b_cracking_degree"]) + str(row["b_cracking_extent"])
        de2 = str(row["b_edge_restraints_degree"]) + str(
            row["b_edge_restraints_extent"]
        )
        de3 = str(row["b_pumping_degree"]) + str(row["b_pumping_extent"])
        de4 = str(row["b_rutting_degree"]) + str(row["b_rutting_extent"])
        de5 = str(row["b_failures_degree"]) + str(row["b_failures_extent"])
        de6 = str(row["b_potholes_degree"]) + str(row["b_potholes_extent"])
        de7 = str(row["b_patching_degree"]) + str(row["b_patching_extent"])
        de8 = str(row["b_reinstatements_degree"]) + str(row["b_reinstatements_extent"])
        de9 = str(row["b_surface_integrity_degree"]) + str(
            row["b_surface_integrity_extent"]
        )
        de10 = str(row["b_jointingmaterial_degree"]) + str(
            row["b_jointingmaterial_extent"]
        )
        de11 = str(row["b_undulation_degree"]) + str(row["b_undulation_extent"])

        de13 = str(row["b_riding_quality"] * 10)
        de12 = str(row["b_skid_resistance"] * 10)
        de14 = str(row["b_surface_drainage"] * 10)
        de15 = str(row["b_shoulders_unpaved"] * 10)
        de16 = str(row["b_shoulders_paved"] * 10)

        de1 = list(df2.loc[df2["de"] == str(de1)]["cracking"])[0]
        de2 = list(df2.loc[df2["de"] == str(de1)]["edge_restraints"])[0]
        de3 = list(df2.loc[df2["de"] == str(de1)]["pumping"])[0]
        de4 = list(df2.loc[df2["de"] == str(de1)]["rutting"])[0]
        de5 = list(df2.loc[df2["de"] == str(de1)]["failures"])[0]
        de6 = list(df2.loc[df2["de"] == str(de1)]["potholes"])[0]
        de7 = list(df2.loc[df2["de"] == str(de1)]["patching"])[0]
        de8 = list(df2.loc[df2["de"] == str(de1)]["reinstatements"])[0]
        de9 = list(df2.loc[df2["de"] == str(de1)]["surface_integrity"])[0]
        de10 = list(df2.loc[df2["de"] == str(de1)]["joint_sand"])[0]
        de11 = list(df2.loc[df2["de"] == str(de1)]["undulation"])[0]
        de12 = list(df2.loc[df2["de"] == str(de1)]["riding_quality"])[0]
        de13 = list(df2.loc[df2["de"] == str(de1)]["skid_resistance"])[0]
        de14 = list(df2.loc[df2["de"] == str(de1)]["surface_drainage"])[0]
        de15 = list(df2.loc[df2["de"] == str(de1)]["shoulders_unpaved"])[0]
        de16 = list(df2.loc[df2["de"] == str(de1)]["shoulders_paved"])[0]

        sorted_list = sorted(
            [
                de1,
                de2,
                de3,
                de4,
                de5,
                de6,
                de7,
                de8,
                de9,
                de10,
                de11,
                de12,
                de13,
                de14,
                de15,
                de16,
            ],
            reverse=True,
        )

        stci = round(
            100
            - sorted_list[0]
            - 0.3 * sorted_list[1]
            - 0.1 * sorted_list[2]
            - 0.05 * sorted_list[3]
            - 0.05 * sorted_list[4]
            - 0.05 * sorted_list[5]
        )

        df["fci_deduct"] = stci
        df["structural_condition_index_stci"] = stci

    return df


def stci_deduct_conc_calc(df):
    filter_cols = [col for col in df if col.startswith("c_")]
    filter_cols = filter_cols.append("assessment_id")
    df = df.loc[:, df[filter_cols]]
    return df


def stci_deduct_unpaved_calc(df):
    filter_cols = [col for col in df if col.startswith("u_")]
    filter_cols = filter_cols.append("assessment_id")
    df = df.loc[:, df[filter_cols]]
    return df


def stci_deduct_flex_calc(df):
    filter_cols = [col for col in df if col.startswith("f_")]
    filter_cols = filter_cols.append("assessment_id")
    df = df.loc[:, df[filter_cols]]
    return df


# ###########################################
# ###########################################


def pci_deduct_Block_calc(df):
    return df


def pci_deduct_conc_calc(df):
    return df


def pci_deduct_flex_calc(df):
    return df


# ###########################################
# ###########################################


def sci_calc(df):
    return df


def sci_flex_deduct_calc(df):
    return df


def vgi_deduct_calc(df):
    return df


def vci_deduct_calc(df):
    return df


def main():
    df = gpd.GeoDataFrame.from_postgis(QRY, ENGINE, geom_col="geometry")
    for col in df.columns:
        try:
            df[col] = df[col].astype(int).fillna(0)
        except:
            pass

    fci_block_lookup_df = pd.read_sql_query(FCI_BLOCK_LOOKUP_QRY, ENGINE)

    df = calculate_mni(df)
    pass


if __name__ == "__main__":
    main()
