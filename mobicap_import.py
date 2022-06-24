import csv
import psycopg2
import os, glob
import pandas as pd
from sqlalchemy import create_engine

conn = psycopg2.connect(
    host="localhost",
    database="asset_management_master",
    user="postgres",
    password="$admin")

####################################################################
#### GET COLUMN NAMES FROM DATABASE FOR VISUAL INSPECTION TABLES ###
####################################################################

def get_col_names():
	flex_column_names = []
	block_column_names = []
	concrete_column_names = []
	unpaved_column_names = []
	with conn as connection:
		with connection.cursor() as cursor:
			cursor.execute("Select * FROM rams.tbl_visual_inspection_flexible LIMIT 0")
			flex_column_names = [desc[0] for desc in cursor.description]
			cursor.execute("Select * FROM rams.tbl_visual_inspection_block LIMIT 0")
			block_column_names = [desc[0] for desc in cursor.description]
			cursor.execute("Select * FROM rams.tbl_visual_inspection_concrete LIMIT 0")
			concrete_column_names = [desc[0] for desc in cursor.description]
			cursor.execute("Select * FROM rams.tbl_visual_inspection_unpaved LIMIT 0")
			unpaved_column_names = [desc[0] for desc in cursor.description]

	conn.close()
	return flex_column_names,block_column_names,concrete,unpaved_column_names

########################################################################################################
#### The code below merges all the CSV files and saves it to a merged CSV file in the same directory ###
########################################################################################################

#### GET ALL FILE NAMES
def getfiles(path):
	mobicap_flex = []
	mobicap_block = []
	mobicap_conc = []
	mobicap_unpaved = []
	for root, dirs, files in os.walk(path):
		for name in files:
			if name == glob.glob("F_Form_*.csv"):
				p = os.path.join(root, name)
				mobicap_flex.append(p)
			elif name == glob.glob("B_Form_*.csv"):
				p = os.path.join(root, name)
				mobicap_block.append(p)
			elif name == glob.glob("J_Form_*.csv"):
				p = os.path.join(root, name)
				mobicap_conc.append(p)
			elif name == glob.glob("U_Form_*.csv"):
				p = os.path.join(root, name)
				mobicap_unpaved.append(p)
	# mobicap_flex = glob.glob(os.path.join(path, "F_Form_*.csv"))
	# mobicap_block = glob.glob(os.path.join(path, "B_Form_*.csv"))
	# mobicap_conc = glob.glob(os.path.join(path, "J_Form_*.csv"))
	# mobicap_unpaved = glob.glob(os.path.join(path, "U_Form_*.csv"))
	return mobicap_flex, mobicap_block, mobicap_conc, mobicap_unpaved



def drop(files):
	all_df = []
	for f in files:
		df = pd.read_csv(f, sep=',')
		all_df.append(df)
	df = pd.concat(all_df, ignore_index=True)
	df = pd.DataFrame.drop_duplicates(merged_df)
	return df

def merge(df_flex,df_block,df_conc,df_unpaved)
	

def reorganize(merged_df, column_names, table_name):
	df = pd.DataFrame(merged_df)
	df.reindex(column_names, axis='columns')
	engine = create_engine('postgresql://postgres:$admin@localhost:5432/asset_management_template')
	df.to_sql(table_name, engine, index=False, if_exists='append')

def main():


if __name__ == '__main__':
	f = str(sys.argv[1])
	# path = "C:/Users/MB2705851/Desktop/Temp Excel/"
	all_files = glob.glob(os.path.join(path, "visin_*.csv"))

	pass
