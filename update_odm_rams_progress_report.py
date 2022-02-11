import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

from win10toast import ToastNotifier

CSV = r"https://web.fulcrumapp.com/shares/48fc49435c5a0199.csv"
WORKBOOK = r"C:\Users\MB2705851\OneDrive - Surbana Jurong Private Limited\PROJECTS\C1579_RRAMS Overberg District Mun\2021\visual_inspection_odm_2021.xlsx"
SHEETNAME = "visual_inspection_odm_2021"
toast = ToastNotifier()
toast.show_toast(
    "SCRIPT RUNNING",
    "ODM Progress Document Updating",
    duration=10,
)

df = pd.read_csv(CSV, low_memory=False)

df.drop(
    [
        "gps_altitude",
        "gps_horizontal_accuracy",
        "gps_vertical_accuracy",
        "gps_speed",
        "gps_course",
        "gps_course",
    ],
    axis=1,
    inplace=True,
)

wb = load_workbook(WORKBOOK, data_only=True)
ws = wb[SHEETNAME]

rows = dataframe_to_rows(df, index=False, header=True)

for r_idx, row in enumerate(rows, 1):
    for c_idx, value in enumerate(row, 1):
        ws.cell(row=r_idx, column=c_idx, value=value)

wb.save(WORKBOOK)

toast.show_toast(
    "SCRIPT RAN SUCCESSFULLY",
    "ODM Progress Document Updated",
    duration=10,
)
