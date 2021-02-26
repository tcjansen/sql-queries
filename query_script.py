import csv


# data table of the RV planet parameters
INPUT_TABLE = "./tic_parameters/RVtargets_1-28-2020.csv"
# column names of conditional parameters for the two databases
SQL_CONDITIONS = ["Teff", "Tmag", "logg", "rad", "mass"]
CSV_KEYS = ["st_teff", "sy_tmag", "st_logg", "st_rad",
			"st_mass"]
sql_condition_strs = ["tic." + i for i in SQL_CONDITIONS]
# beginning of each query
QUERY_START = (r"SELECT TOP 1000 tic.ID," +
				",".join(sql_condition_strs) +
				" FROM TESS_v81 AS tic")


def _write_condition(csv_value, csv_uerr, csv_lerr, g, g_uerr, g_lerr):
	# all variables are type(str)
	f = csv_value
	# agree within 2 sigma
	f_upperlim = str(float(f) + 2 * float(csv_uerr))
	f_lowerlim = str(float(f) - 2 * float(csv_lerr))
	first_condition = ("(" + g + " > " + f + ") AND (" +
						g + " - 2 * " + g_lerr + " < " +
						f_upperlim + ")")
	second_condition = ("(" + g + " < " + f + ") AND (" +
						g + " + 2 * " + g_uerr + " > " +
						f_lowerlim + ")")
	return ("(" + first_condition + ") OR (" + second_condition + ")")


# write a separate sql query for each TIC ID
# with respective conditions
with open("tic_sql_query.txt", 'w') as w:
	with open(INPUT_TABLE) as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			tic = row["tic_id"]
			query = (QUERY_START + " INTO " + tic[:3] + tic [4:] +
					 "\nWHERE (tic.objType = STAR)")
			for k in range(len(CSV_KEYS)):
				query += " AND ("
				if row[CSV_KEYS[k]] == "":
					continue
				elif row[CSV_KEYS[k] + "err2"] == "":
					print('err2 true')
					l_err_csv = "0"
				elif row[CSV_KEYS[k] + "err1"] == "":
					print('err1 true')
					u_err_csv = "0"
				else:
					value_csv = row[CSV_KEYS[k]]
					l_err_csv = str(abs(float(row[CSV_KEYS[k] + "err2"])))
					u_err_csv = row[CSV_KEYS[k] + "err1"]

				sql_param = "tic." + SQL_CONDITIONS[k]
				sql_err_param = "tic." + "e_" + SQL_CONDITIONS[k]

				query += "(" + _write_condition(value_csv, u_err_csv, l_err_csv,
										 sql_param, sql_err_param,
										 sql_err_param) + "))"

			w.write(query + "\n\n")
	csvfile.close()
w.close()

