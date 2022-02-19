from audioop import reverse
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
import pandasql
from sympy import E1

ENGINE = create_engine(
    "postgresql://postgres:$admin@localhost:5432/asset_management_master"
)

TABLE = "road_visual_assessment_main"
SCHEMA = "assessment"

QRY = """SELECT * FROM assessment.road_visual_assessment rva WHERE rva.visual_condition_index_vci IS NULL 
		AND rva.visual_gravel_index_vgi IS NULL AND rva.structural_condition_index_stci IS null"""
ASSETS_QRY = """SELECT * FROM infrastructure.asset where asset_type_id = 2"""
RISFSA_QRY = """SELECT * FROM lookups.risfsa"""
RAINFALL_QRY = """SELECT * FROM base_layers.mean_rainfall"""

FCI_BLOCK_LOOKUP_QRY = """SELECT * FROM lookups.fci_block_deduct"""
FCI_CONC_LOOKUP_QRY = """SELECT * FROM lookups.fci_conc_deduct"""
FCI_FLEX_LOOKUP_QRY = """SELECT * FROM lookups.fci_flex_deduct"""
FCI_UNPAVED_LOOKUP_QRY = """SELECT * FROM lookups.fci_grav_deduct"""

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
    df["index"] = None
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

    df["index"] = round(
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

    df = df[["visual_assessment_id", "index"]]

    return df


# ###########################################
# ###########################################


def deduct_block_calc(df, df2):
    filter_cols = [
        col for col in df if col.startswith("b_") or col == "visual_assessment_id"
    ]
    df = df.loc[:, filter_cols].fillna(0)
    df["index"] = None

    for idx, row in df.iterrows():
        if str(row["b_cracking_degree"]) == "0" or str(row["b_cracking_extent"]) == "0":
            de1 = "00"
        else:
            de1 = str(row["b_cracking_degree"]) + str(row["b_cracking_extent"])

        if (
            str(row["b_edge_restraints_degree"]) == "0"
            or str(row["b_edge_restraints_extent"]) == "0"
        ):
            de2 = "00"
        else:
            de2 = str(row["b_edge_restraints_degree"]) + str(
                row["b_edge_restraints_extent"]
            )

        if str(row["b_pumping_degree"]) == "0" or str(row["b_pumping_extent"]) == "0":
            de3 = "00"
        else:
            de3 = str(row["b_pumping_degree"]) + str(row["b_pumping_extent"])

        if str(row["b_rutting_degree"]) == "0" or str(row["b_rutting_extent"]) == "0":
            de4 = "00"
        else:
            de4 = str(row["b_rutting_degree"]) + str(row["b_rutting_extent"])

        if str(row["b_failures_degree"]) == "0" or str(row["b_failures_extent"]) == "0":
            de5 = "00"
        else:
            de5 = str(row["b_failures_degree"]) + str(row["b_failures_extent"])

        if str(row["b_potholes_degree"]) == "0" or str(row["b_potholes_extent"]) == "0":
            de6 = "00"
        else:
            de6 = str(row["b_potholes_degree"]) + str(row["b_potholes_extent"])

        if str(row["b_patching_degree"]) == "0" or str(row["b_patching_extent"]) == "0":
            de7 = "00"
        else:
            de7 = str(row["b_patching_degree"]) + str(row["b_patching_extent"])

        if (
            str(row["b_reinstatements_degree"]) == "0"
            or str(row["b_reinstatements_extent"]) == "0"
        ):
            de8 = "00"
        else:
            de8 = str(row["b_reinstatements_degree"]) + str(
                row["b_reinstatements_extent"]
            )

        if (
            str(row["b_surface_integrity_degree"]) == "0"
            or str(row["b_surface_integrity_extent"]) == "0"
        ):
            de9 = "00"
        else:
            de9 = str(row["b_surface_integrity_degree"]) + str(
                row["b_surface_integrity_extent"]
            )

        if (
            str(row["b_jointingmaterial_degree"]) == "0"
            or str(row["b_jointingmaterial_extent"]) == "0"
        ):
            de10 = "00"
        else:
            de10 = str(row["b_jointingmaterial_degree"]) + str(
                row["b_jointingmaterial_extent"]
            )

        if (
            str(row["b_undulation_degree"]) == "0"
            or str(row["b_undulation_extent"]) == "0"
        ):
            de11 = "00"
        else:
            de11 = str(row["b_undulation_degree"]) + str(row["b_undulation_extent"])

        d12 = str(row["b_riding_quality"] * 10)
        d13 = str(row["b_skid_resistance"] * 10)
        d14 = str(row["b_surface_drainage"] * 10)
        d15 = str(row["b_shoulders_unpaved"] * 10)
        d16 = str(row["b_shoulders_paved"] * 10)

        if d12[:1] == "1":
            e12 = "1"
        elif d12[1:2] == "0" or d12[1:2] is None:
            e12 = "1"
        elif len(d12) == 1:
            e12 = "0"
        else:
            e12 = d12[1:2]

        if d13[:1] == "1":
            e13 = "1"
        elif d13[1:2] == "0" or d13[1:2] is None:
            e13 = "1"
        elif len(d13) == 1:
            e13 = "0"
        else:
            e13 = d13[1:2]

        if d14[:1] == "1":
            e14 = "1"
        elif d14[1:2] == "0" or d14[1:2] is None:
            e14 = "1"
        elif len(d14) == 1:
            e14 = "0"
        else:
            e14 = d14[1:2]

        if d15[:1] == "1":
            e15 = "1"
        elif d15[1:2] == "0" or d15[1:2] is None:
            e15 = "1"
        elif len(d15) == 1:
            e15 = "0"
        else:
            e15 = d15[1:2]

        if d16[:1] == "1":
            e16 = "1"
        elif d16[1:2] == "0":
            e16 = "1"
        elif len(d16) == 1:
            e16 = "0"
        else:
            e16 = d16[1:2]

        de12 = d12[:1] + e12
        de13 = d13[:1] + e13
        de14 = d14[:1] + e14
        de15 = d15[:1] + e15
        de16 = d16[:1] + e16

        de1 = list(df2.loc[df2["de"] == str(de1)]["cracking"])[0]
        de2 = list(df2.loc[df2["de"] == str(de2)]["edge_restraints"])[0]
        de3 = list(df2.loc[df2["de"] == str(de3)]["pumping"])[0]
        de4 = list(df2.loc[df2["de"] == str(de4)]["rutting"])[0]
        de5 = list(df2.loc[df2["de"] == str(de5)]["failures"])[0]
        de6 = list(df2.loc[df2["de"] == str(de6)]["potholes"])[0]
        de7 = list(df2.loc[df2["de"] == str(de7)]["patching"])[0]
        de8 = list(df2.loc[df2["de"] == str(de8)]["reinstatements"])[0]
        de9 = list(df2.loc[df2["de"] == str(de9)]["surface_integrity"])[0]
        de10 = list(df2.loc[df2["de"] == str(de10)]["joint_sand"])[0]
        de11 = list(df2.loc[df2["de"] == str(de11)]["undulation"])[0]
        de12 = list(df2.loc[df2["de"] == str(de12)]["riding_quality"])[0]
        de13 = list(df2.loc[df2["de"] == str(de13)]["skid_resistance"])[0]
        de14 = list(df2.loc[df2["de"] == str(de14)]["surface_drainage"])[0]
        de15 = list(df2.loc[df2["de"] == str(de15)]["shoulders_unpaved"])[0]
        de16 = list(df2.loc[df2["de"] == str(de16)]["shoulders_paved"])[0]

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

        index = round(
            100
            - sorted_list[0]
            - 0.3 * sorted_list[1]
            - 0.1 * sorted_list[2]
            - 0.05 * sorted_list[3]
            - 0.05 * sorted_list[4]
            - 0.05 * sorted_list[5]
        )

        df.at[idx, "index"] = index

    df.drop(
        df.columns.difference(["visual_assessment_id", "index"]), axis=1, inplace=True
    )

    return df


def deduct_conc_calc(df, df2):
    filter_cols = [
        col for col in df if col.startswith("c_") or col == "visual_assessment_id"
    ]
    df = df.loc[:, filter_cols].fillna(0)
    df["index"] = None

    for idx, row in df.iterrows():

        if (
            str(row["c_joint_sealant_degree"]) == "0"
            or str(row["c_joint_sealant_extent"]) == "0"
        ):
            de1 = "00"
        else:
            de1 = str(row["c_joint_sealant_degree"]) + str(
                row["c_joint_sealant_extent"]
            )

        if (
            str(row["c_undulation_settlement_degree"]) == "0"
            or str(row["c_undulation_settlement_extent"]) == "0"
        ):
            de2 = "00"
        else:
            de2 = str(row["c_undulation_settlement_degree"]) + str(
                row["c_undulation_settlement_extent"]
            )

        if (
            str(row["c_joint_associated_cracks_degree"]) == "0"
            or str(row["c_joint_associated_cracks_extent"]) == "0"
        ):
            de3 = "00"
        else:
            de3 = str(row["c_joint_associated_cracks_degree"]) + str(
                row["c_joint_associated_cracks_extent"]
            )

        if (
            str(row["c_spalled_joints_degree"]) == "0"
            or str(row["c_spalled_joints_extent"]) == "0"
        ):
            de4 = "00"
        else:
            de4 = str(row["c_spalled_joints_degree"]) + str(
                row["c_spalled_joints_extent"]
            )

        if (
            str(row["c_cracks_random_degree"]) == "0"
            or str(row["c_cracks_random_extent"]) == "0"
        ):
            de5 = "00"
        else:
            de5 = str(row["c_cracks_random_degree"]) + str(
                row["c_cracks_random_extent"]
            )

        if (
            str(row["c_cracks_longitudinal_degree"]) == "0"
            or str(row["c_cracks_longitudinal_extent"]) == "0"
        ):
            de6 = "00"
        else:
            de6 = str(row["c_cracks_longitudinal_degree"]) + str(
                row["c_cracks_longitudinal_extent"]
            )

        if (
            str(row["c_cracks_transverse_degree"]) == "0"
            or str(row["c_cracks_transverse_extent"]) == "0"
        ):
            de7 = "00"
        else:
            de7 = str(row["c_cracks_transverse_degree"]) + str(
                row["c_cracks_transverse_extent"]
            )

        if (
            str(row["c_corner_breaks_degree"]) == "0"
            or str(row["c_corner_breaks_extent"]) == "0"
        ):
            de8 = "00"
        else:
            de8 = str(row["c_corner_breaks_degree"]) + str(
                row["c_corner_breaks_extent"]
            )

        if (
            str(row["c_cracks_cluster_degree"]) == "0"
            or str(row["c_cracks_cluster_extent"]) == "0"
        ):
            de9 = "00"
        else:
            de9 = str(row["c_cracks_cluster_degree"]) + str(
                row["c_cracks_cluster_extent"]
            )

        if (
            str(row["c_cracked_slabs_degree"]) == "0"
            or str(row["c_cracked_slabs_extent"]) == "0"
        ):
            de10 = "00"
        else:
            de10 = str(row["c_cracked_slabs_degree"]) + str(
                row["c_cracked_slabs_extent"]
            )

        if (
            str(row["c_shattered_slabs_degree"]) == "0"
            or str(row["f_surface_failure_extent"]) == "0"
        ):
            de11 = "00"
        else:
            de11 = str(row["c_shattered_slabs_degree"]) + str(
                row["c_shattered_slabs_extent"]
            )

        if str(row["c_faulting_degree"]) == "0" or str(row["c_faulting_extent"]) == "0":
            de12 = "00"
        else:
            de12 = str(row["c_faulting_degree"]) + str(row["c_faulting_extent"])

        if str(row["c_failures_degree"]) == "0" or str(row["c_failures_extent"]) == "0":
            de13 = "00"
        else:
            de13 = str(row["c_failures_degree"]) + str(row["c_failures_extent"])

        if str(row["c_patching_degree"]) == "0" or str(row["c_patching_extent"]) == "0":
            de14 = "00"
        else:
            de14 = str(row["c_patching_degree"]) + str(row["c_patching_extent"])

        if (
            str(row["c_punchouts_degree"]) == "0"
            or str(row["c_punchouts_extent"]) == "0"
        ):
            de15 = "00"
        else:
            de15 = str(row["c_punchouts_degree"]) + str(row["c_punchouts_extent"])

        if (
            str(row["f_surface_failure_degree"]) == "0"
            or str(row["c_pumping_extent"]) == "0"
        ):
            de16 = "00"
        else:
            de16 = str(row["c_pumping_degree"]) + str(row["c_pumping_extent"])

        d17 = str(row["c_riding_quality"] * 10)
        d18 = str(row["c_skid_resistance"] * 10)
        d19 = str(row["c_shoulders_unpaved"] * 10)
        d20 = str(row["c_shoulders_paved"] * 10)

        if d17[:1] == "1":
            e17 = "1"
        elif d17[1:2] == "0" or d17[1:2] is None:
            e17 = "1"
        elif len(d17) == 1:
            e17 = "0"
        else:
            e17 = d17[1:2]

        if d18[:1] == "1":
            e18 = "1"
        elif d18[1:2] == "0" or d83[1:2] is None:
            e18 = "1"
        elif len(d18) == 1:
            e18 = "0"
        else:
            e18 = d18[1:2]

        if d19[:1] == "1":
            e19 = "1"
        elif d19[1:2] == "0" or d19[1:2] is None:
            e19 = "1"
        elif len(d19) == 1:
            e19 = "0"
        else:
            e19 = d19[1:2]

        if d20[:1] == "1":
            e20 = "1"
        elif d20[1:2] == "0" or d20[1:2] is None:
            e20 = "1"
        elif len(d20) == 1:
            e20 = "0"
        else:
            e20 = d20[1:2]

        de17 = d17[:1] + e17
        de18 = d18[:1] + e18
        de19 = d19[:1] + e19
        de20 = d20[:1] + e20

        de1 = list(df2.loc[df2["de"] == str(de1)]["joint_sealant"])[0]
        de2 = list(df2.loc[df2["de"] == str(de2)]["concrete_durability"])[0]
        de3 = list(df2.loc[df2["de"] == str(de3)]["joint_associated_cracks"])[0]
        de4 = list(df2.loc[df2["de"] == str(de4)]["spalled_joints"])[0]
        de5 = list(df2.loc[df2["de"] == str(de5)]["cracks_random"])[0]
        de6 = list(df2.loc[df2["de"] == str(de6)]["cracks_longit"])[0]
        de7 = list(df2.loc[df2["de"] == str(de7)]["cracks_transverse"])[0]
        de8 = list(df2.loc[df2["de"] == str(de8)]["corner_breaks"])[0]
        de9 = list(df2.loc[df2["de"] == str(de9)]["cracks_cluster"])[0]
        de10 = list(df2.loc[df2["de"] == str(de10)]["cracked_slabs"])[0]
        de11 = list(df2.loc[df2["de"] == str(de11)]["shattered_slabs"])[0]
        de12 = list(df2.loc[df2["de"] == str(de12)]["faulting"])[0]
        de13 = list(df2.loc[df2["de"] == str(de13)]["failures"])[0]
        de14 = list(df2.loc[df2["de"] == str(de14)]["patching"])[0]
        de15 = list(df2.loc[df2["de"] == str(de15)]["punchouts"])[0]
        de16 = list(df2.loc[df2["de"] == str(de16)]["pumping"])[0]
        de17 = list(df2.loc[df2["de"] == str(de17)]["riding_quality"])[0]
        de18 = list(df2.loc[df2["de"] == str(de18)]["skid_resistance"])[0]
        de19 = list(df2.loc[df2["de"] == str(de19)]["shoulders_unpaved"])[0]
        de20 = list(df2.loc[df2["de"] == str(de20)]["shoulders_paved"])[0]

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
                de17,
                de18,
                de19,
                de20,
            ],
            reverse=True,
        )

        index = round(
            100
            - sorted_list[0]
            - 0.3 * sorted_list[1]
            - 0.1 * sorted_list[2]
            - 0.05 * sorted_list[3]
            - 0.05 * sorted_list[4]
            - 0.05 * sorted_list[5]
        )

        df.at[idx, "index"] = index

    df.drop(
        df.columns.difference(["visual_assessment_id", "index"]), axis=1, inplace=True
    )

    return df


def deduct_unpaved_calc(df, df2):
    filter_cols = [
        col for col in df if col.startswith("u_") or col == "visual_assessment_id"
    ]
    df = df.loc[:, filter_cols].fillna(0)
    df["index"] = None

    for idx, row in df.iterrows():

        if row["u_material_quality"] == "0":
            e1 = str("0")
        else:
            e1 = str("3")

        if row["u_material_quality"] == "0":
            e2 = str("0")
        else:
            e2 = str("3")

        de1 = str(row["u_material_quality"]) + str(e1)
        de2 = str(row["u_material_quantity"]) + str(e2)

        if str(row["u_potholes_degree"]) == "0" or str(row["u_potholes_extent"]) == "0":
            de3 = "00"
        else:
            de3 = str(row["u_potholes_degree"]) + str(row["u_potholes_extent"])

        if (
            str(row["u_corrugations_degree"]) == "0"
            or str(row["u_corrugations_extent"]) == "0"
        ):
            de4 = "00"
        else:
            de4 = str(row["u_corrugations_degree"]) + str(row["u_corrugations_extent"])

        if str(row["u_rutting_degree"]) == "0" or str(row["u_rutting_extent"]) == "0":
            de15 = "00"
        else:
            de5 = str(row["u_rutting_degree"]) + str(row["u_rutting_extent"])

        if (
            str(row["u_loosematerial_degree"]) == "0"
            or str(row["u_loosematerial_extent"]) == "0"
        ):
            de6 = "00"
        else:
            de6 = str(row["u_loosematerial_degree"]) + str(
                row["u_loosematerial_extent"]
            )

        if (
            str(row["u_stoniness_fixed_degree"]) == "0"
            or str(row["u_stoniness_fixed_extent"]) == "0"
        ):
            de7 = "00"
        else:
            de7 = str(row["u_stoniness_fixed_degree"]) + str(
                row["u_stoniness_fixed_extent"]
            )

        if (
            str(row["u_stoniness_loose_degree"]) == "0"
            or str(row["u_stoniness_loose_extent"]) == "0"
        ):
            de8 = "00"
        else:
            de8 = str(row["u_stoniness_loose_degree"]) + str(
                row["u_stoniness_loose_extent"]
            )

        if (
            str(row["u_erosion_longitudinal_degree"]) == "0"
            or str(row["u_erosion_longitudinal_extent"]) == "0"
        ):
            de9 = "00"
        else:
            de9 = str(row["u_erosion_longitudinal_degree"]) + str(
                row["u_erosion_longitudinal_extent"]
            )

        if (
            str(row["u_erosion_transverse_degree"]) == "0"
            or str(row["u_erosion_transverse_extent"]) == "0"
        ):
            de10 = "00"
        else:
            de10 = str(row["u_erosion_transverse_degree"]) + str(
                row["u_erosion_transverse_extent"]
            )

        d11 = str(row["u_roughness"] * 10)
        d12 = str(row["u_transverse_profile"] * 10)
        d13 = str(row["u_transverse_profile"] * 10)
        d14 = str(row["u_trafficability"] * 10)
        d15 = str(row["u_safety"] * 10)
        d16 = str(row["u_drainage_road"] * 10)
        d17 = str(row["u_drainage_roadside"] * 10)

        if d11[:1] == "1":
            e11 = "1"
        elif d11[1:2] == "0" or d11[1:2] is None:
            e11 = "1"
        elif len(d11) == 1:
            e11 = "0"
        else:
            e11 = d11[1:2]

        if d12[:1] == "1":
            e12 = "1"
        elif d12[1:2] == "0" or d12[1:2] is None:
            e12 = "1"
        elif len(d12) == 1:
            e12 = "0"
        else:
            e12 = d12[1:2]

        if d13[:1] == "1":
            e13 = "1"
        elif d13[1:2] == "0" or d13[1:2] is None:
            e13 = "1"
        elif len(d13) == 1:
            e13 = "0"
        else:
            e13 = d13[1:2]

        if d14[:1] == "1":
            e14 = "1"
        elif d14[1:2] == "0" or d14[1:2] is None:
            e14 = "1"
        elif len(d14) == 1:
            e14 = "0"
        else:
            e14 = d14[1:2]

        if d15[:1] == "1":
            e15 = "1"
        elif d15[1:2] == "0" or d15[1:2] is None:
            e15 = "1"
        elif len(d15) == 1:
            e15 = "0"
        else:
            e15 = d15[1:2]

        if d16[:1] == "1":
            e16 = "1"
        elif d16[1:2] == "0":
            e16 = "1"
        elif len(d16) == 1:
            e16 = "0"
        else:
            e16 = d16[1:2]

        if d17[:1] == "1":
            e17 = "1"
        elif d17[1:2] == "0":
            e17 = "1"
        elif len(d17) == 1:
            e17 = "0"
        else:
            e17 = d17[1:2]

        de11 = d11[:1] + e11
        de12 = d12[:1] + e12
        de13 = d13[:1] + e13
        de14 = d14[:1] + e14
        de15 = d15[:1] + e15
        de16 = d16[:1] + e16
        de17 = d17[:1] + e17

        de1 = list(df2.loc[df2["de"] == str(de1)]["unpaved_gravel_quality"])[0]
        de2 = list(df2.loc[df2["de"] == str(de2)]["gravel_thickness"])[0]
        de3 = list(df2.loc[df2["de"] == str(de3)]["potholes"])[0]
        de4 = list(df2.loc[df2["de"] == str(de4)]["corrugations"])[0]
        de5 = list(df2.loc[df2["de"] == str(de5)]["rutting"])[0]
        de6 = list(df2.loc[df2["de"] == str(de6)]["loose_material"])[0]
        de7 = list(df2.loc[df2["de"] == str(de7)]["stones_fixed"])[0]
        de8 = list(df2.loc[df2["de"] == str(de8)]["stones_loose"])[0]
        de9 = list(df2.loc[df2["de"] == str(de9)]["erosion_longitudinal"])[0]
        de10 = list(df2.loc[df2["de"] == str(de10)]["erosion_transverse"])[0]

        de11 = list(df2.loc[df2["de"] == str(de11)]["unpaved_riding_quality"])[0]
        de12 = list(df2.loc[df2["de"] == str(de12)]["surface_profile"])[0]
        de13 = list(df2.loc[df2["de"] == str(de13)]["cross_section"])[0]
        de14 = list(df2.loc[df2["de"] == str(de14)]["traffickability"])[0]
        de15 = list(df2.loc[df2["de"] == str(de15)]["safety"])[0]
        de16 = list(df2.loc[df2["de"] == str(de16)]["drainage_on_road"])[0]
        de17 = list(df2.loc[df2["de"] == str(de17)]["drainage_roadside"])[0]

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
                de17,
            ],
            reverse=True,
        )

        index = round(
            100
            - sorted_list[0]
            - 0.3 * sorted_list[1]
            - 0.1 * sorted_list[2]
            - 0.05 * sorted_list[3]
            - 0.05 * sorted_list[4]
            - 0.05 * sorted_list[5]
        )

        df.at[idx, "index"] = index

    df.drop(
        df.columns.difference(["visual_assessment_id", "index"]), axis=1, inplace=True
    )

    return df


def deduct_flex_calc(df, df2):
    filter_cols = [
        col for col in df if col.startswith("f_") or col == "visual_assessment_id"
    ]
    df = df.loc[:, filter_cols].fillna(0)
    df["index"] = None

    for idx, row in df.iterrows():

        sla = str(row["f_stone_loss_active"])

        if (
            str(row["f_surface_failure_degree"]) == "0"
            or str(row["f_surface_failure_extent"]) == "0"
        ):
            de1 = "00"
        else:
            de1 = str(row["f_surface_failure_degree"]) + str(
                row["f_surface_failure_extent"]
            )

        if (
            str(row["f_surface_cracking_degree"]) == "0"
            or str(row["f_surface_cracking_extent"]) == "0"
        ):
            de2 = "00"
        else:
            de2 = str(row["f_surface_cracking_degree"]) + str(
                row["f_surface_cracking_extent"]
            )

        if (
            str(row["f_stone_loss_degree"]) == "0"
            or str(row["f_stone_loss_extent"]) == "0"
        ):
            de3 = "00"
        else:
            de3 = str(row["f_stone_loss_degree"]) + str(row["f_stone_loss_extent"])

        if (
            str(row["f_surface_patching_degree"]) == "0"
            or str(row["f_surface_patching_extent"]) == "0"
        ):
            de4 = "00"
        else:
            de4 = str(row["f_surface_patching_degree"]) + str(
                row["f_surface_patching_extent"]
            )

        if (
            str(row["f_binder_condition_degree"]) == "0"
            or str(row["f_binder_condition_extent"]) == "0"
        ):
            de5 = "00"
        else:
            de5 = str(row["f_binder_condition_degree"]) + str(
                row["f_binder_condition_extent"]
            )

        if str(row["f_bleeding_degree"]) == "0" or str(row["f_bleeding_extent"]) == "0":
            de6 = "00"
        else:
            de6 = str(row["f_bleeding_degree"]) + str(row["f_bleeding_extent"])

        if (
            str(row["f_block_cracks_degree"]) == "0"
            or str(row["f_block_cracks_extent"]) == "0"
        ):
            de7 = "00"
        else:
            de7 = str(row["f_block_cracks_degree"]) + str(row["f_block_cracks_extent"])

        if (
            str(row["f_longitudinal_cracks_degree"]) == "0"
            or str(row["f_longitudinal_cracks_extent"]) == "0"
        ):
            de8 = "00"
        else:
            de8 = str(row["f_longitudinal_cracks_degree"]) + str(
                row["f_longitudinal_cracks_extent"]
            )

        if (
            str(row["f_transverse_cracks_degree"]) == "0"
            or str(row["f_transverse_cracks_extent"]) == "0"
        ):
            de9 = "00"
        else:
            de9 = str(row["f_transverse_cracks_degree"]) + str(
                row["f_transverse_cracks_extent"]
            )

        if (
            str(row["f_crocodile_cracks_degree"]) == "0"
            or str(row["f_crocodile_cracks_extent"]) == "0"
        ):
            de10 = "00"
        else:
            de10 = str(row["f_crocodile_cracks_degree"]) + str(
                row["f_crocodile_cracks_extent"]
            )

        if str(row["f_pumping_degree"]) == "0" or str(row["f_pumping_extent"]) == "0":
            de11 = "00"
        else:
            de11 = str(row["f_pumping_degree"]) + str(row["f_pumping_extent"])

        if str(row["f_rutting_degree"]) == "0" or str(row["f_rutting_extent"]) == "0":
            de12 = "00"
        else:
            de12 = str(row["f_rutting_degree"]) + str(row["f_rutting_extent"])

        if str(row["f_shoving_degree"]) == "0" or str(row["f_shoving_extent"]) == "0":
            de13 = "00"
        else:
            de13 = str(row["f_shoving_degree"]) + str(row["f_shoving_extent"])

        if (
            str(row["f_undulation_degree"]) == "0"
            or str(row["f_undulation_extent"]) == "0"
        ):
            de14 = "00"
        else:
            de14 = str(row["f_undulation_degree"]) + str(row["f_undulation_extent"])

        if str(row["f_patching_degree"]) == "0" or str(row["f_patching_extent"]) == "0":
            de15 = "00"
        else:
            de15 = str(row["f_patching_degree"]) + str(row["f_patching_extent"])

        if str(row["f_failures_degree"]) == "0" or str(row["f_failures_extent"]) == "0":
            de16 = "00"
        else:
            de16 = str(row["f_failures_degree"]) + str(row["f_failures_extent"])

        if (
            str(row["f_edge_breaking_degree"]) == "0"
            or str(row["f_edge_breaking_extent"]) == "0"
        ):
            de17 = "00"
        else:
            de17 = str(row["f_edge_breaking_degree"]) + str(
                row["f_edge_breaking_extent"]
            )

        d18 = str(row["f_riding_quality"] * 10)
        d19 = str(row["f_skid_resistance"] * 10)
        d20 = str(row["f_surface_drainage"] * 10)
        d21 = str(row["f_shoulders_unpaved"] * 10)
        d22 = str(row["f_shoulders_paved"] * 10)

        if d18[:1] == "1":
            e18 = "1"
        elif d18[1:2] == "0" or d18[1:2] is None:
            e18 = "1"
        elif len(d18) == 1:
            e18 = "0"
        else:
            e18 = d18[1:2]

        if d19[:1] == "1":
            e19 = "1"
        elif d19[1:2] == "0" or d19[1:2] is None:
            e19 = "1"
        elif len(d19) == 1:
            e19 = "0"
        else:
            e19 = d19[1:2]

        if d20[:1] == "1":
            e20 = "1"
        elif d20[1:2] == "0" or d20[1:2] is None:
            e20 = "1"
        elif len(d20) == 1:
            e20 = "0"
        else:
            e20 = d20[1:2]

        if d21[:1] == "1":
            e21 = "1"
        elif d21[1:2] == "0" or d21[1:2] is None:
            e21 = "1"
        elif len(d21) == 1:
            e21 = "0"
        else:
            e21 = d21[1:2]

        if d22[:1] == "1":
            e22 = "1"
        elif d22[1:2] == "0":
            e22 = "1"
        elif len(d22) == 1:
            e22 = "0"
        else:
            e22 = d22[1:2]

        de18 = d18[:1] + e18
        de19 = d19[:1] + e19
        de20 = d20[:1] + e20
        de21 = d21[:1] + e21
        de22 = d22[:1] + e22

        de1 = list(df2.loc[df2["de"] == str(de1)]["surface_failure"])[0]
        de2 = list(df2.loc[df2["de"] == str(de2)]["surface_cracking"])[0]
        if sla == "A":
            de3 = list(df2.loc[df2["de"] == str(de3)]["stone_loss_active"])[0]
        else:
            de3 = list(df2.loc[df2["de"] == str(de3)]["stone_loss"])[0]
        de4 = list(df2.loc[df2["de"] == str(de4)]["surface_patching"])[0]
        de5 = list(df2.loc[df2["de"] == str(de5)]["dry"])[0]
        de6 = list(df2.loc[df2["de"] == str(de6)]["bleeding"])[0]
        de7 = list(df2.loc[df2["de"] == str(de7)]["block_cracks"])[0]
        de8 = list(df2.loc[df2["de"] == str(de8)]["longitudinal_cracks"])[0]
        de9 = list(df2.loc[df2["de"] == str(de9)]["transverse_cracks"])[0]
        de10 = list(df2.loc[df2["de"] == str(de10)]["crocodile_cracks"])[0]

        de11 = list(df2.loc[df2["de"] == str(de11)]["pumping"])[0]
        de12 = list(df2.loc[df2["de"] == str(de12)]["rutting"])[0]
        de13 = list(df2.loc[df2["de"] == str(de13)]["shoving"])[0]
        de14 = list(df2.loc[df2["de"] == str(de14)]["undulation"])[0]
        de15 = list(df2.loc[df2["de"] == str(de15)]["patching"])[0]
        de16 = list(df2.loc[df2["de"] == str(de16)]["failures"])[0]
        de17 = list(df2.loc[df2["de"] == str(de17)]["edge_breaking"])[0]
        de18 = list(df2.loc[df2["de"] == str(de18)]["riding_quality"])[0]
        de19 = list(df2.loc[df2["de"] == str(de19)]["skid_resistance"])[0]
        de20 = list(df2.loc[df2["de"] == str(de20)]["surface_drainage"])[0]
        de21 = list(df2.loc[df2["de"] == str(de21)]["shoulders_unpaved"])[0]
        de22 = list(df2.loc[df2["de"] == str(de22)]["shoulders_paved"])[0]

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
                de17,
                de18,
                de19,
                de20,
                de21,
                de22,
            ],
            reverse=True,
        )

        index = round(
            100
            - sorted_list[0]
            - 0.3 * sorted_list[1]
            - 0.1 * sorted_list[2]
            - 0.05 * sorted_list[3]
            - 0.05 * sorted_list[4]
            - 0.05 * sorted_list[5]
        )

        df.at[idx, "index"] = index

    df.drop(
        df.columns.difference(["visual_assessment_id", "index"]), axis=1, inplace=True
    )

    return df


# ###########################################
# ###########################################


def vci_sci_calc(df, df2, dem_dict):
    filter_cols = [
        col for col in df if col.startswith("f_") or col == "visual_assessment_id"
    ]
    df = df.loc[:, filter_cols].fillna(0)
    df["index"] = None

    dem_dict = dem_dict

    for idx, row in df.iterrows():

        sla = row["f_stone_loss_active"]

        d1 = row["f_surface_patching_degree"]
        e1 = row["f_surface_patching_extent"]
        d2 = row["f_surface_failure_degree"]
        e2 = row["f_surface_failure_extent"]
        d3 = row["f_surface_cracking_degree"]
        e3 = row["f_surface_cracking_extent"]
        d4 = row["f_stone_loss_degree"]
        e4 = row["f_stone_loss_extent"]
        d5 = row["f_binder_condition_degree"]
        e5 = row["f_binder_condition_extent"]
        d6 = row["f_bleeding_degree"]
        e6 = row["f_bleeding_extent"]
        d7 = row["f_block_cracks_degree"]
        e7 = row["f_block_cracks_extent"]
        d8 = row["f_longitudinal_cracks_degree"]
        e8 = row["f_longitudinal_cracks_extent"]
        d9 = row["f_transverse_cracks_degree"]
        e9 = row["f_transverse_cracks_extent"]
        d10 = row["f_crocodile_cracks_degree"]
        e10 = row["f_crocodile_cracks_extent"]
        d11 = row["f_pumping_degree"]
        e11 = row["f_pumping_extent"]
        d12 = row["f_rutting_degree"]
        e12 = row["f_rutting_extent"]
        d13 = row["f_shoving_degree"]
        e13 = row["f_shoving_extent"]
        d14 = row["f_undulation_degree"]
        e14 = row["f_undulation_extent"]
        d15 = row["f_patching_degree"]
        e15 = row["f_patching_extent"]
        d16 = row["f_failures_degree"]
        e16 = row["f_failures_extent"]
        d17 = row["f_edge_breaking_degree"]
        e17 = row["f_edge_breaking_extent"]
        d18 = row["f_riding_quality"]
        e18 = 3
        d19 = row["f_skid_resistance"]
        e19 = 3
        d20 = row["f_surface_drainage"]
        e20 = 3
        d21 = row["f_shoulders_unpaved"]
        e21 = 3
        d22 = row["f_shoulders_paved"]
        e22 = 3

        if sla == "A":
            sigma_dem = sum(dem_dict.values()) - dem_dict.get("dem5N")
            sigma_de = (
                d1
                * e1 ** list(df2.loc[df2["id"] == "2"]["y"])[0]
                * list(df2.loc[df2["id"] == "2"]["weight"])[0]
                * list(df2.loc[df2["id"] == "2"]["small_n"])[0]
                + d2
                * e2 ** list(df2.loc[df2["id"] == "3"]["y"])[0]
                * list(df2.loc[df2["id"] == "3"]["weight"])[0]
                * list(df2.loc[df2["id"] == "3"]["small_n"])[0]
                + d3
                * e3 ** list(df2.loc[df2["id"] == "4"]["y"])[0]
                * list(df2.loc[df2["id"] == "4"]["weight"])[0]
                * list(df2.loc[df2["id"] == "4"]["small_n"])[0]
                + d4
                * e4 ** list(df2.loc[df2["id"] == "5A"]["y"])[0]
                * list(df2.loc[df2["id"] == "5A"]["weight"])[0]
                * list(df2.loc[df2["id"] == "5A"]["small_n"])[0]
                + d5
                * e5 ** list(df2.loc[df2["id"] == "6"]["y"])[0]
                * list(df2.loc[df2["id"] == "6"]["weight"])[0]
                * list(df2.loc[df2["id"] == "6"]["small_n"])[0]
                + d6
                * e6 ** list(df2.loc[df2["id"] == "7"]["y"])[0]
                * list(df2.loc[df2["id"] == "7"]["weight"])[0]
                * list(df2.loc[df2["id"] == "7"]["small_n"])[0]
                + d7
                * e7 ** list(df2.loc[df2["id"] == "8"]["y"])[0]
                * list(df2.loc[df2["id"] == "8"]["weight"])[0]
                * list(df2.loc[df2["id"] == "8"]["small_n"])[0]
                + d8
                * e8 ** list(df2.loc[df2["id"] == "9"]["y"])[0]
                * list(df2.loc[df2["id"] == "9"]["weight"])[0]
                * list(df2.loc[df2["id"] == "9"]["small_n"])[0]
                + d9
                * e9 ** list(df2.loc[df2["id"] == "10"]["y"])[0]
                * list(df2.loc[df2["id"] == "10"]["weight"])[0]
                * list(df2.loc[df2["id"] == "10"]["small_n"])[0]
                + d10
                * e10 ** list(df2.loc[df2["id"] == "11"]["y"])[0]
                * list(df2.loc[df2["id"] == "11"]["weight"])[0]
                * list(df2.loc[df2["id"] == "11"]["small_n"])[0]
                + d11
                * e11 ** list(df2.loc[df2["id"] == "12"]["y"])[0]
                * list(df2.loc[df2["id"] == "12"]["weight"])[0]
                * list(df2.loc[df2["id"] == "12"]["small_n"])[0]
                + d12
                * e12 ** list(df2.loc[df2["id"] == "13"]["y"])[0]
                * list(df2.loc[df2["id"] == "13"]["weight"])[0]
                * list(df2.loc[df2["id"] == "13"]["small_n"])[0]
                + d13
                * e13 ** list(df2.loc[df2["id"] == "14"]["y"])[0]
                * list(df2.loc[df2["id"] == "14"]["weight"])[0]
                * list(df2.loc[df2["id"] == "14"]["small_n"])[0]
                + d14
                * e14 ** list(df2.loc[df2["id"] == "15"]["y"])[0]
                * list(df2.loc[df2["id"] == "15"]["weight"])[0]
                * list(df2.loc[df2["id"] == "15"]["small_n"])[0]
                + d15
                * e15 ** list(df2.loc[df2["id"] == "16"]["y"])[0]
                * list(df2.loc[df2["id"] == "16"]["weight"])[0]
                * list(df2.loc[df2["id"] == "16"]["small_n"])[0]
                + d16
                * e16 ** list(df2.loc[df2["id"] == "17"]["y"])[0]
                * list(df2.loc[df2["id"] == "17"]["weight"])[0]
                * list(df2.loc[df2["id"] == "17"]["small_n"])[0]
                + d17
                * e17 ** list(df2.loc[df2["id"] == "18"]["y"])[0]
                * list(df2.loc[df2["id"] == "18"]["weight"])[0]
                * list(df2.loc[df2["id"] == "18"]["small_n"])[0]
                + d18
                * e18 ** list(df2.loc[df2["id"] == "19"]["y"])[0]
                * list(df2.loc[df2["id"] == "19"]["weight"])[0]
                * list(df2.loc[df2["id"] == "19"]["small_n"])[0]
                + d19
                * e19 ** list(df2.loc[df2["id"] == "20"]["y"])[0]
                * list(df2.loc[df2["id"] == "20"]["weight"])[0]
                * list(df2.loc[df2["id"] == "20"]["small_n"])[0]
                + d20
                * e20 ** list(df2.loc[df2["id"] == "21"]["y"])[0]
                * list(df2.loc[df2["id"] == "21"]["weight"])[0]
                * list(df2.loc[df2["id"] == "21"]["small_n"])[0]
                + d21
                * e21 ** list(df2.loc[df2["id"] == "22"]["y"])[0]
                * list(df2.loc[df2["id"] == "22"]["weight"])[0]
                * list(df2.loc[df2["id"] == "22"]["small_n"])[0]
                + d22
                * e22 ** list(df2.loc[df2["id"] == "23"]["y"])[0]
                * list(df2.loc[df2["id"] == "23"]["weight"])[0]
                * list(df2.loc[df2["id"] == "23"]["small_n"])[0]
            )
        else:
            sigma_dem = sum(dem_dict.values()) - dem_dict.get("dem5A")
            sigma_de = (
                d1
                * e1 ** list(df2.loc[df2["id"] == "2"]["y"])[0]
                * list(df2.loc[df2["id"] == "2"]["weight"])[0]
                * list(df2.loc[df2["id"] == "2"]["small_n"])[0]
                + d2
                * e2 ** list(df2.loc[df2["id"] == "3"]["y"])[0]
                * list(df2.loc[df2["id"] == "3"]["weight"])[0]
                * list(df2.loc[df2["id"] == "3"]["small_n"])[0]
                + d3
                * e3 ** list(df2.loc[df2["id"] == "4"]["y"])[0]
                * list(df2.loc[df2["id"] == "4"]["weight"])[0]
                * list(df2.loc[df2["id"] == "4"]["small_n"])[0]
                + d4
                * e4 ** list(df2.loc[df2["id"] == "5N"]["y"])[0]
                * list(df2.loc[df2["id"] == "5N"]["weight"])[0]
                * list(df2.loc[df2["id"] == "5N"]["small_n"])[0]
                + d5
                * e5 ** list(df2.loc[df2["id"] == "6"]["y"])[0]
                * list(df2.loc[df2["id"] == "6"]["weight"])[0]
                * list(df2.loc[df2["id"] == "6"]["small_n"])[0]
                + d6
                * e6 ** list(df2.loc[df2["id"] == "7"]["y"])[0]
                * list(df2.loc[df2["id"] == "7"]["weight"])[0]
                * list(df2.loc[df2["id"] == "7"]["small_n"])[0]
                + d7
                * e7 ** list(df2.loc[df2["id"] == "8"]["y"])[0]
                * list(df2.loc[df2["id"] == "8"]["weight"])[0]
                * list(df2.loc[df2["id"] == "8"]["small_n"])[0]
                + d8
                * e8 ** list(df2.loc[df2["id"] == "9"]["y"])[0]
                * list(df2.loc[df2["id"] == "9"]["weight"])[0]
                * list(df2.loc[df2["id"] == "9"]["small_n"])[0]
                + d9
                * e9 ** list(df2.loc[df2["id"] == "10"]["y"])[0]
                * list(df2.loc[df2["id"] == "10"]["weight"])[0]
                * list(df2.loc[df2["id"] == "10"]["small_n"])[0]
                + d10
                * e10 ** list(df2.loc[df2["id"] == "11"]["y"])[0]
                * list(df2.loc[df2["id"] == "11"]["weight"])[0]
                * list(df2.loc[df2["id"] == "11"]["small_n"])[0]
                + d11
                * e11 ** list(df2.loc[df2["id"] == "12"]["y"])[0]
                * list(df2.loc[df2["id"] == "12"]["weight"])[0]
                * list(df2.loc[df2["id"] == "12"]["small_n"])[0]
                + d12
                * e12 ** list(df2.loc[df2["id"] == "13"]["y"])[0]
                * list(df2.loc[df2["id"] == "13"]["weight"])[0]
                * list(df2.loc[df2["id"] == "13"]["small_n"])[0]
                + d13
                * e13 ** list(df2.loc[df2["id"] == "14"]["y"])[0]
                * list(df2.loc[df2["id"] == "14"]["weight"])[0]
                * list(df2.loc[df2["id"] == "14"]["small_n"])[0]
                + d14
                * e14 ** list(df2.loc[df2["id"] == "15"]["y"])[0]
                * list(df2.loc[df2["id"] == "15"]["weight"])[0]
                * list(df2.loc[df2["id"] == "15"]["small_n"])[0]
                + d15
                * e15 ** list(df2.loc[df2["id"] == "16"]["y"])[0]
                * list(df2.loc[df2["id"] == "16"]["weight"])[0]
                * list(df2.loc[df2["id"] == "16"]["small_n"])[0]
                + d16
                * e16 ** list(df2.loc[df2["id"] == "17"]["y"])[0]
                * list(df2.loc[df2["id"] == "17"]["weight"])[0]
                * list(df2.loc[df2["id"] == "17"]["small_n"])[0]
                + d17
                * e17 ** list(df2.loc[df2["id"] == "18"]["y"])[0]
                * list(df2.loc[df2["id"] == "18"]["weight"])[0]
                * list(df2.loc[df2["id"] == "18"]["small_n"])[0]
                + d18
                * e18 ** list(df2.loc[df2["id"] == "19"]["y"])[0]
                * list(df2.loc[df2["id"] == "19"]["weight"])[0]
                * list(df2.loc[df2["id"] == "19"]["small_n"])[0]
                + d19
                * e19 ** list(df2.loc[df2["id"] == "20"]["y"])[0]
                * list(df2.loc[df2["id"] == "20"]["weight"])[0]
                * list(df2.loc[df2["id"] == "20"]["small_n"])[0]
                + d20
                * e20 ** list(df2.loc[df2["id"] == "21"]["y"])[0]
                * list(df2.loc[df2["id"] == "21"]["weight"])[0]
                * list(df2.loc[df2["id"] == "21"]["small_n"])[0]
                + d21
                * e21 ** list(df2.loc[df2["id"] == "22"]["y"])[0]
                * list(df2.loc[df2["id"] == "22"]["weight"])[0]
                * list(df2.loc[df2["id"] == "22"]["small_n"])[0]
                + d22
                * e22 ** list(df2.loc[df2["id"] == "23"]["y"])[0]
                * list(df2.loc[df2["id"] == "23"]["weight"])[0]
                * list(df2.loc[df2["id"] == "23"]["small_n"])[0]
            )

        index = 100 * (1 - sigma_de / sigma_dem)
        index = (0.04 * index + 0.0006 * index ** 2) ** 2
        index = round(index)

        df.at[idx, "index"] = index

    df.drop(
        df.columns.difference(["visual_assessment_id", "index"]), axis=1, inplace=True
    )

    return df


def stci_merge(df, index_df):
    df = pd.merge(
        df,
        index_df[["visual_assessment_id", "index"]],
        on="visual_assessment_id",
        how="left",
    )
    df["fci_deduct"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["fci_deduct"] == None
        else x["fci_deduct"],
        axis=1,
    )
    df["structural_condition_index_stci"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["structural_condition_index_stci"] == None
        else x["structural_condition_index_stci"],
        axis=1,
    )
    df.drop(["index"], axis=1, inplace=True)
    return df


def pci_merge(df, index_df):
    df = pd.merge(
        df,
        index_df[["visual_assessment_id", "index"]],
        on="visual_assessment_id",
        how="left",
    )
    df["pci_deduct"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["pci_deduct"] == None
        else x["pci_deduct"],
        axis=1,
    )
    df["surface_condition_index_sci"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["surface_condition_index_sci"] == None
        else x["surface_condition_index_sci"],
        axis=1,
    )
    df.drop(["index"], axis=1, inplace=True)
    return df


def main():
    df = gpd.GeoDataFrame.from_postgis(QRY, ENGINE, geom_col="geometry")
    for col in df.columns:
        try:
            df[col] = df[col].astype(int).fillna(0)
        except:
            pass

    fci_flex_lookup_df = pd.read_sql_query(FCI_FLEX_LOOKUP_QRY, ENGINE)
    fci_block_lookup_df = pd.read_sql_query(FCI_BLOCK_LOOKUP_QRY, ENGINE)
    fci_conc_lookup_df = pd.read_sql_query(FCI_CONC_LOOKUP_QRY, ENGINE)
    fci_unpaved_lookup_df = pd.read_sql_query(FCI_UNPAVED_LOOKUP_QRY, ENGINE)

    pci_flex_lookup_df = pd.read_sql_query(PCI_FLEX_LOOKUP_QRY, ENGINE)
    pci_block_lookup_df = pd.read_sql_query(PCI_BLOCK_LOOKUP_QRY, ENGINE)
    pci_conc_lookup_df = pd.read_sql_query(PCI_CONC_LOOKUP_QRY, ENGINE)

    sci_flex_lookup_df = pd.read_sql_query(SCI_FLEX_LOOKUP_QRY, ENGINE)

    sci_weights_lookup_df = pd.read_sql_query(SCI_WEIGHTS_QRY, ENGINE)
    vci_weights_lookup_df = pd.read_sql_query(VCI_WEIGHTS_QRY, ENGINE)
    vgi_weights_lookup_df = pd.read_sql_query(VGI_DEDUCT_QRY, ENGINE)

    vci_dem_dict = {}
    for idx, row in vci_weights_lookup_df.iterrows():
        vci_dem_dict["dem" + row["id"]] = (
            row["d_max"] * row["e_max"] ** row["y"] * row["weight"] * row["small_n"]
        )

    sci_dem_dict = {}
    for idx, row in sci_weights_lookup_df.iterrows():
        sci_dem_dict["dem" + row["id"]] = (
            row["d_max"] * row["e_max"] ** row["y"] * row["weight"] * row["small_n"]
        )

    index_df = deduct_flex_calc(df, fci_flex_lookup_df)
    df = stci_merge(df, index_df)

    index_df = deduct_block_calc(df, fci_block_lookup_df)
    df = stci_merge(df, index_df)

    index_df = deduct_conc_calc(df, fci_conc_lookup_df)
    df = stci_merge(df, index_df)

    index_df = deduct_unpaved_calc(df, fci_unpaved_lookup_df)
    df = stci_merge(df, index_df)

    index_df = deduct_block_calc(df, pci_block_lookup_df)
    df = pci_merge(df, index_df)

    index_df = deduct_conc_calc(df, pci_conc_lookup_df)
    df = pci_merge(df, index_df)

    index_df = deduct_unpaved_calc(df, vgi_weights_lookup_df)
    df = pd.merge(
        df,
        index_df[["visual_assessment_id", "index"]],
        on="visual_assessment_id",
        how="left",
    )
    df["visual_condition_index_vci"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["visual_condition_index_vci"] == None
        else x["visual_condition_index_vci"],
        axis=1,
    )
    df["visual_gravel_index_vgi"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["visual_gravel_index_vgi"] == None
        else x["visual_gravel_index_vgi"],
        axis=1,
    )
    df.drop(["index"], axis=1, inplace=True)

    index_df = vci_sci_calc(df, vci_weights_lookup_df, vci_dem_dict)
    df = pd.merge(
        df,
        index_df[["visual_assessment_id", "index"]],
        on="visual_assessment_id",
        how="left",
    )
    df["visual_condition_index_vci"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["visual_condition_index_vci"] == None
        else x["visual_condition_index_vci"],
        axis=1,
    )
    df.drop(["index"], axis=1, inplace=True)

    index_df = vci_sci_calc(df, sci_weights_lookup_df, sci_dem_dict)
    df = pd.merge(
        df,
        index_df[["visual_assessment_id", "index"]],
        on="visual_assessment_id",
        how="left",
    )
    df["surface_condition_index_sci"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["surface_condition_index_sci"] == None
        else x["surface_condition_index_sci"],
        axis=1,
    )
    df.drop(["index"], axis=1, inplace=True)

    index_df = deduct_flex_calc(df, pci_flex_lookup_df)
    df = pd.merge(
        df,
        index_df[["visual_assessment_id", "index"]],
        on="visual_assessment_id",
        how="left",
    )
    df["pci_deduct"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["pci_deduct"] == None
        else x["pci_deduct"],
        axis=1,
    )
    df.drop(["index"], axis=1, inplace=True)

    index_df = deduct_flex_calc(df, sci_flex_lookup_df)
    df = pd.merge(
        df,
        index_df[["visual_assessment_id", "index"]],
        on="visual_assessment_id",
        how="left",
    )
    df["surface_condition_index_sci"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["surface_condition_index_sci"] == None
        else x["surface_condition_index_sci"],
        axis=1,
    )
    df.drop(["index"], axis=1, inplace=True)

    index_df = deduct_flex_calc(df, sci_weights_lookup_df)
    df = pd.merge(
        df,
        index_df[["visual_assessment_id", "index"]],
        on="visual_assessment_id",
        how="left",
    )
    df["sci_deduct"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["sci_deduct"] == None
        else x["sci_deduct"],
        axis=1,
    )
    df.drop(["index"], axis=1, inplace=True)

    index_df = calculate_mni(df)
    df = pd.merge(
        df,
        index_df[["visual_assessment_id", "index"]],
        on="visual_assessment_id",
        how="left",
    )
    df["fci_deduct"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["fci_deduct"] == None
        else x["fci_deduct"],
        axis=1,
    )
    df["maintenance_need_index_mni"] = df.apply(
        lambda x: x["index"]
        if x["index"] > 0 and x["maintenance_need_index_mni"] == None
        else x["maintenance_need_index_mni"],
        axis=1,
    )
    df.drop(["index"], axis=1, inplace=True)

    df.to_sql(
        TABLE,
        ENGINE,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        method=psql_insert_copy,
    )


if __name__ == "__main__":
    main()
