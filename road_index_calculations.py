from audioop import reverse
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
import pandasql
from sympy import E1

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


def deduct_block_calc(df, df2):
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

        df["index"] = index

    return df


def deduct_conc_calc(df, df2):
    filter_cols = [col for col in df if col.startswith("c_")]
    filter_cols = filter_cols.append("assessment_id")
    df = df.loc[:, df[filter_cols]]

    for idx, row in df.iterrows():

        de1 = str(row["c_joint_sealant_degree"]) + str(row["c_joint_sealant_extent"])
        de2 = str(row["c_undulation_settlement_degree"]) + str(
            row["c_undulation_settlement_extent"]
        )
        de3 = str(row["c_joint_associated_cracks_degree"]) + str(
            row["c_joint_associated_cracks_extent"]
        )
        de4 = str(row["c_spalled_joints_degree"]) + str(row["c_spalled_joints_extent"])
        de5 = str(row["c_cracks_random_degree"]) + str(row["c_cracks_random_extent"])
        de6 = str(row["c_cracks_longitudinal_degree"]) + str(
            row["c_cracks_longitudinal_extent"]
        )
        de7 = str(row["c_cracks_transverse_degree"]) + str(
            row["c_cracks_transverse_extent"]
        )
        de8 = str(row["c_corner_breaks_degree"]) + str(row["c_corner_breaks_extent"])
        de9 = str(row["c_cracks_cluster_degree"]) + str(row["c_cracks_cluster_extent"])
        de10 = str(row["c_cracked_slabs_degree"]) + str(row["c_cracked_slabs_extent"])
        de11 = str(row["c_shattered_slabs_degree"]) + str(
            row["c_shattered_slabs_extent"]
        )

        de12 = str(row["c_faulting_degree"]) + str(row["c_faulting_extent"])
        de13 = str(row["c_failures_degree"]) + str(row["c_failures_extent"])
        de14 = str(row["c_patching_degree"]) + str(row["c_patching_extent"])
        de15 = str(row["c_punchouts_degree"]) + str(row["c_punchouts_extent"])
        de16 = str(row["c_pumping_degree"]) + str(row["c_pumping_extent"])

        de17 = str(row["c_riding_quality"] * 10)
        de18 = str(row["c_skid_resistance"] * 10)
        de19 = str(row["c_shoulders_unpaved"] * 10)
        de20 = str(row["c_shoulders_paved"] * 10)

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

        df["index"] = index

    return df


def deduct_unpaved_calc(df, df2):
    filter_cols = [col for col in df if col.startswith("u_")]
    filter_cols = filter_cols.append("assessment_id")
    df = df.loc[:, df[filter_cols]]

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
        de3 = str(row["u_potholes_degree"]) + str(row["u_potholes_extent"])
        de4 = str(row["u_corrugations_degree"]) + str(row["u_corrugations_extent"])
        de5 = str(row["u_rutting_degree"]) + str(row["u_rutting_extent"])
        de6 = str(row["u_loosematerial_degree"]) + str(row["u_loosematerial_extent"])
        de7 = str(row["u_stoniness_fixed_degree"]) + str(
            row["u_stoniness_fixed_extent"]
        )
        de8 = str(row["u_stoniness_loose_degree"]) + str(
            row["u_stoniness_loose_extent"]
        )
        de9 = str(row["u_erosion_longitudinal_degree"]) + str(
            row["u_erosion_longitudinal_extent"]
        )
        de10 = str(row["u_erosion_transverse_degree"]) + str(
            row["u_erosion_transverse_extent"]
        )

        de11 = str(row["u_roughness"] * 10)
        de12 = str(row["u_transverse_profile"] * 10)
        de13 = str(row["u_transverse_profile"] * 10)
        de14 = str(row["u_trafficability"] * 10)
        de15 = str(row["u_safety"] * 10)
        de16 = str(row["u_drainage_road"] * 10)
        de17 = str(row["u_drainage_roadside"] * 10)

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

        df["index"] = index

    return df


def deduct_flex_calc(df, df2):
    filter_cols = [col for col in df if col.startswith("f_")]
    filter_cols = filter_cols.append("assessment_id")
    df = df.loc[:, df[filter_cols]]

    for idx, row in df.iterrows():

        de1 = str(row["f_surface_failure_degree"]) + str(
            row["f_surface_failure_extent"]
        )
        de2 = str(row["f_surface_cracking_degree"]) + str(
            row["f_surface_cracking_extent"]
        )
        sla = str(row["f_stone_loss_active"])
        de3 = str(row["f_stone_loss_degree"]) + str(row["f_stone_loss_extent"])
        de4 = str(row["f_surface_patching_degree"]) + str(
            row["f_surface_patching_extent"]
        )
        de5 = str(row["f_binder_condition_degree"]) + str(
            row["f_binder_condition_extent"]
        )
        de6 = str(row["f_bleeding_degree"]) + str(row["f_bleeding_extent"])
        de7 = str(row["f_block_cracks_degree"]) + str(row["f_block_cracks_extent"])
        de8 = str(row["f_longitudinal_cracks_degree"]) + str(
            row["f_longitudinal_cracks_extent"]
        )
        de9 = str(row["f_transverse_cracks_degree"]) + str(
            row["f_transverse_cracks_extent"]
        )
        de10 = str(row["f_crocodile_cracks_degree"]) + str(
            row["f_crocodile_cracks_extent"]
        )

        de11 = str(row["f_pumping_degree"]) + str(row["f_pumping_extent"])
        de12 = str(row["f_rutting_degree"]) + str(row["f_rutting_extent"])
        de13 = str(row["f_shoving_degree"]) + str(row["f_shoving_extent"])
        de14 = str(row["f_undulation_degree"]) + str(row["f_undulation_extent"])
        de15 = str(row["f_patching_degree"]) + str(row["f_patching_extent"])
        de16 = str(row["f_failures_degree"]) + str(row["f_failures_extent"])
        de17 = str(row["f_edge_breaking_degree"]) + str(row["f_edge_breaking_extent"])

        de18 = str(row["f_riding_quality"] * 10)
        de19 = str(row["f_skid_resistance"] * 10)
        de20 = str(row["f_surface_drainage"] * 10)
        de21 = str(row["f_shoulders_unpaved"] * 10)
        de22 = str(row["f_shoulders_paved"] * 10)

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

        df["index"] = index

    return df


def vci_deduct_calc(df, df2):
    filter_cols = [col for col in df if col.startswith("f_")]
    filter_cols = filter_cols.append("assessment_id")
    df = df.loc[:, df[filter_cols]].fillna(0)

    df2["dem"] = df2["d_max"] * df2["e_max"] ^ df2["y"] * df2["weight"] * df2["small_n"]

    for idx, row in df.iterrows():

        sla = df["f_stone_loss_active"]

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

        degree_list = [
            "f_surface_patching_degree",
            "f_surface_failure_degree",
            "f_surface_cracking_degree",
            "f_stone_loss_degree",
            "f_stone_loss_degree",
        ]

    if sla == "A":
        sigma_dem = (
            df2["dem"].sum(axis=1) - list(df2.loc[df2["id"] == '5N']["dem"])[0]
        )
        sigma_de = None
    else:
        sigma_dem = (
            df2["dem"].sum(axis=1) - list(df2.loc[df2["id"] == "5A"]["dem"])[0]
        )
        sigma_de = None

    return df


def vgi_deduct_calc(df, df2):
    filter_cols = [col for col in df if col.startswith("u_")]
    filter_cols = filter_cols.append("assessment_id")
    df = df.loc[:, df[filter_cols]]

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


if __name__ == "__main__":
    main()
