import sqlite3
from datetime import timedelta
import datetime
import csv

if __name__ == "__main__" :
	print("Hello")

	conn = sqlite3.connect("./sql_stock.db")
	cur = conn.cursor()

	sel_code_dic = "select * from code_dic"
	cur.execute(sel_code_dic)

	rows = cur.fetchall()
	
	oneday = timedelta(days = 1)
	dt = datetime.date(2018, 7, 9)
	ds_dst = datetime.date(2007, 12, 31)

	day_list = ["", "",]
	while ds_dst != dt:
		day_list.append(dt.strftime("%Y%m%d"))
		dt = dt - oneday

	with open("./csv_test.csv", "w", encoding="cp949", newline ="\n") as f  :
			wr = csv.writer(f)
			wr.writerow(day_list)

	i = 0;
	for r in rows:
		#print("%s : %s"%(r[0], r[1]))

		table_name = r[0]
	
		value_list = [r[0], r[1], ]
		for d in day_list[2:]:
		
			today = d
			print(d)
			print("r[0] : " + r[0])
			stock_total_value_qry = "select stock_total_value from \"" + table_name + "\" where date = \"" + today + "\""
			
			cur.execute(stock_total_value_qry)
			res = cur.fetchall()

			val = 0
			
			for st in res :
				val = st[0]
			#print(val)

			value_list.append(val)
			
		with open("./csv_test.csv", "a", encoding="cp949", newline = "\n") as f  :
			wr = csv.writer(f, quoting = csv.QUOTE_ALL)
			wr.writerow(value_list)

		#i += 1
		#if i > 5 :
		#	break;

	conn.close();
