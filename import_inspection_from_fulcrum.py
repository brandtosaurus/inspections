import pandas as pd

# import requests
# from urllib3 import request
# import json
from sqlalchemy import create_engine

# import psycopg2

from io import StringIO
import csv

from win10toast import ToastNotifier

toast = ToastNotifier()
toast.show_toast(
    "SCRIPT RUNNING",
    "Inserting records from FulcrumApp",
    duration=10,
)

TABLE = "road_visual_assessment_main"
CREATED_TABLE = "road_visual_assessment_created"

SCHEMA = "assessment"
OUTPATH = r"~\Desktop\Temp\Temp Excel\Fulcrum\test.csv"
OUTPATH2 = r"~\Desktop\Temp\Temp Excel\Fulcrum\test2.csv"

CSV = r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.csv"
CSV_ANCILLARY = (
    r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.csv?child=ancillary_assets"
)

JSON = r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.json"

GEOJSON = r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.geojson"
GEOJSON_ANCILLARY = (
    r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.geojson?child=ancillary_assets"
)

# GP_ENGINE = create_engine(
#     "postgresql://postgres:Lin3@r1in3!431@linearline.dedicated.co.za:5432/gauteng"
# )
ENGINE = create_engine(
    "postgresql://postgres:$admin@localhost:5432/asset_management_master"
)


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


df = pd.read_csv(CSV, low_memory=False)
ancillary_data = pd.read_csv(CSV_ANCILLARY, low_memory=False)

for col in df.columns:
    try:
        df[col] = df[col].astype(int).fillna(0)
    except:
        pass

df[
    [
        "version",
        "samrecordid",
        "asset_type_id",
        "project_id",
        "f_surface_failure_degree",
        "f_surface_failure_extent",
        "f_surface_patching_degree",
        "f_surface_patching_extent",
        "f_surface_cracking_degree",
        "f_surface_cracking_extent",
        "f_stone_loss_degree",
        "f_stone_loss_extent",
        "f_binder_condition_degree",
        "f_binder_condition_extent",
        "f_bleeding_degree",
        "f_bleeding_extent",
        "f_shoving_degree",
        "f_shoving_extent",
        "f_block_cracks_degree",
        "f_block_cracks_extent",
        "f_longitudinal_cracks_degree",
        "f_longitudinal_cracks_extent",
        "f_transverse_cracks_degree",
        "f_transverse_cracks_extent",
        "f_crocodile_cracks_degree",
        "f_crocodile_cracks_extent",
        "f_pumping_degree",
        "f_pumping_extent",
        "f_rutting_degree",
        "f_rutting_extent",
        "f_undulation_degree",
        "f_undulation_extent",
        "f_patching_degree",
        "f_patching_extent",
        "f_potholes_degree",
        "f_potholes_extent",
        "f_failures_degree",
        "f_failures_extent",
        "f_riding_quality",
        "f_skid_resistance",
        "f_surface_drainage",
        "f_shoulders_unpaved",
        "f_edge_breaking_degree",
        "f_edge_breaking_extent",
        "f_edgetransversecracksshort_degree",
        "f_edgetransversecracksshort_extent",
        "f_edgedropoff_degree",
        "f_edgedropoff_extent",
        "f_general_condition",
        "f_paved_embayments",
        "f_reinstatements_degree",
        "f_reinstatements_extent",
        "f_shoulders_paved",
        "b_thickness",
        "b_cracking_degree",
        "b_cracking_extent",
        "b_blockabrasion_degree",
        "b_blockabrasion_extent",
        "b_jointingmaterial_degree",
        "b_jointingmaterial_extent",
        "b_edge_restraints_degree",
        "b_edge_restraints_extent",
        "b_rutting_degree",
        "b_rutting_extent",
        "b_undulation_degree",
        "b_undulation_extent",
        "b_potholes_degree",
        "b_potholes_extent",
        "b_roughness_degree",
        "b_skid_resistance",
        "b_surface_drainage",
        "b_shoulders_unpaved",
        "b_shoulders_paved",
        "b_general_condition",
        "b_heightdifferential_degree",
        "b_heightdifferential_extent",
        "b_reinstatements_degree",
        "b_reinstatements_extent",
        "b_failures_degree",
        "b_failures_extent",
        "c_cracks_random_degree",
        "c_cracks_random_extent",
        "c_cracks_transverse_degree",
        "c_cracks_transverse_extent",
        "c_cracks_longitudinal_degree",
        "c_cracks_longitudinal_extent",
        "c_pumping_degree",
        "c_pumping_extent",
        "c_joint_sealant_degree",
        "c_joint_sealant_extent",
        "c_cracks_cluster_degree",
        "c_cracks_cluster_extent",
        "c_undulation_settlement_degree",
        "c_undulation_settlement_extent",
        "c_punchouts_degree",
        "c_punchouts_extent",
        "c_patching_degree",
        "c_patching_extent",
        "c_corner_breaks_degree",
        "c_corner_breaks_extent",
        "c_faulting_degree",
        "c_faulting_extent",
        "c_shattered_slabs_degree",
        "c_shattered_slabs_extent",
        "c_roughness",
        "c_skid_resistance",
        "c_surface_drainage",
        "c_shoulders_unpaved",
        "c_general_condition",
        "c_cracked_slabs_degree",
        "c_cracked_slabs_extent",
        "c_rutting_degree",
        "c_rutting_extent",
        "c_failures_degree",
        "c_failures_extent",
        "c_riding_quality",
        "c_shoulders_paved",
        "u_material_quality",
        "u_subgrade_quality",
        "u_potholes_degree",
        "u_potholes_extent",
        "u_corrugations_degree",
        "u_corrugations_extent",
        "u_rutting_degree",
        "u_rutting_extent",
        "u_loosematerial_degree",
        "u_loosematerial_extent",
        "u_stoniness_fixed_degree",
        "u_stoniness_fixed_extent",
        "u_stoniness_loose_degree",
        "u_stoniness_loose_extent",
        "u_erosion_longitudinal_degree",
        "u_erosion_longitudinal_extent",
        "u_erosion_transverse_degree",
        "u_erosion_transverse_extent",
        "u_roughness",
        "u_trafficability",
        "u_safety",
        "u_drainage_road",
        "u_drainage_roadside",
        "u_general_condition",
        "u_material_quantity",
        "number_of_sidewalks",
        "sidewalk_percent",
        "sidewalk_degree",
        "kerb_percent",
        "number_of_lanes",
        "u_transverse_profile",
    ]
] = (
    df[
        [
            "version",
            "samrecordid",
            "asset_type_id",
            "project_id",
            "f_surface_failure_degree",
            "f_surface_failure_extent",
            "f_surface_patching_degree",
            "f_surface_patching_extent",
            "f_surface_cracking_degree",
            "f_surface_cracking_extent",
            "f_stone_loss_degree",
            "f_stone_loss_extent",
            "f_binder_condition_degree",
            "f_binder_condition_extent",
            "f_bleeding_degree",
            "f_bleeding_extent",
            "f_shoving_degree",
            "f_shoving_extent",
            "f_block_cracks_degree",
            "f_block_cracks_extent",
            "f_longitudinal_cracks_degree",
            "f_longitudinal_cracks_extent",
            "f_transverse_cracks_degree",
            "f_transverse_cracks_extent",
            "f_crocodile_cracks_degree",
            "f_crocodile_cracks_extent",
            "f_pumping_degree",
            "f_pumping_extent",
            "f_rutting_degree",
            "f_rutting_extent",
            "f_undulation_degree",
            "f_undulation_extent",
            "f_patching_degree",
            "f_patching_extent",
            "f_potholes_degree",
            "f_potholes_extent",
            "f_failures_degree",
            "f_failures_extent",
            "f_riding_quality",
            "f_skid_resistance",
            "f_surface_drainage",
            "f_shoulders_unpaved",
            "f_edge_breaking_degree",
            "f_edge_breaking_extent",
            "f_edgetransversecracksshort_degree",
            "f_edgetransversecracksshort_extent",
            "f_edgedropoff_degree",
            "f_edgedropoff_extent",
            "f_general_condition",
            "f_paved_embayments",
            "f_reinstatements_degree",
            "f_reinstatements_extent",
            "f_shoulders_paved",
            "b_thickness",
            "b_cracking_degree",
            "b_cracking_extent",
            "b_blockabrasion_degree",
            "b_blockabrasion_extent",
            "b_jointingmaterial_degree",
            "b_jointingmaterial_extent",
            "b_edge_restraints_degree",
            "b_edge_restraints_extent",
            "b_rutting_degree",
            "b_rutting_extent",
            "b_undulation_degree",
            "b_undulation_extent",
            "b_potholes_degree",
            "b_potholes_extent",
            "b_roughness_degree",
            "b_skid_resistance",
            "b_surface_drainage",
            "b_shoulders_unpaved",
            "b_shoulders_paved",
            "b_general_condition",
            "b_heightdifferential_degree",
            "b_heightdifferential_extent",
            "b_reinstatements_degree",
            "b_reinstatements_extent",
            "b_failures_degree",
            "b_failures_extent",
            "c_cracks_random_degree",
            "c_cracks_random_extent",
            "c_cracks_transverse_degree",
            "c_cracks_transverse_extent",
            "c_cracks_longitudinal_degree",
            "c_cracks_longitudinal_extent",
            "c_pumping_degree",
            "c_pumping_extent",
            "c_joint_sealant_degree",
            "c_joint_sealant_extent",
            "c_cracks_cluster_degree",
            "c_cracks_cluster_extent",
            "c_undulation_settlement_degree",
            "c_undulation_settlement_extent",
            "c_punchouts_degree",
            "c_punchouts_extent",
            "c_patching_degree",
            "c_patching_extent",
            "c_corner_breaks_degree",
            "c_corner_breaks_extent",
            "c_faulting_degree",
            "c_faulting_extent",
            "c_shattered_slabs_degree",
            "c_shattered_slabs_extent",
            "c_roughness",
            "c_skid_resistance",
            "c_surface_drainage",
            "c_shoulders_unpaved",
            "c_general_condition",
            "c_cracked_slabs_degree",
            "c_cracked_slabs_extent",
            "c_rutting_degree",
            "c_rutting_extent",
            "c_failures_degree",
            "c_failures_extent",
            "c_riding_quality",
            "c_shoulders_paved",
            "u_material_quality",
            "u_subgrade_quality",
            "u_potholes_degree",
            "u_potholes_extent",
            "u_corrugations_degree",
            "u_corrugations_extent",
            "u_rutting_degree",
            "u_rutting_extent",
            "u_loosematerial_degree",
            "u_loosematerial_extent",
            "u_stoniness_fixed_degree",
            "u_stoniness_fixed_extent",
            "u_stoniness_loose_degree",
            "u_stoniness_loose_extent",
            "u_erosion_longitudinal_degree",
            "u_erosion_longitudinal_extent",
            "u_erosion_transverse_degree",
            "u_erosion_transverse_extent",
            "u_roughness",
            "u_trafficability",
            "u_safety",
            "u_drainage_road",
            "u_drainage_roadside",
            "u_general_condition",
            "u_material_quantity",
            "number_of_sidewalks",
            "sidewalk_percent",
            "sidewalk_degree",
            "kerb_percent",
            "number_of_lanes",
            "u_transverse_profile",
        ]
    ]
    .fillna(0)
    .astype(int)
)

inspected = df[df["status"] == "inspected"]
inspected["segment_id"] = inspected["asset_id"]

created = df[df["status"] == "created"]
created["asset_id"] = created["fulcrum_id"]
created["segment_id"] = created["fulcrum_id"]

# inspected.drop('ownership', axis=1, inplace=True)
# created.drop('ownership', axis=1, inplace=True)

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
