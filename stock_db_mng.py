import os
import codecs
import datetime
import sqlite3

data_path = "./stock_data/"

def fileToLines(path) :
	info = os.stat(path)
	print(info.st_size)
	
	f = open(path, "rb")
	f.read(3)
	data = f.read(info.st_size - 3)
	f.close()
	lines = data.decode("utf-8")
		
	ll = lines.split("\n")
	return ll
	
def use_codecs(path):
	f = open(path, "rb")
	f.read(3)
	info = os.stat(path)
	data = f.read(info.st_size - 3)
	f.close()

	c = codecs.decode(data, "utf-8")
	
	i = 0
	cl = c.split("\n")
	print(cl[10])
	#for l in cl:
	#	print("%d : %s" % (i, l))
	#	i += 1	
	

if __name__ == "__main__" :
	
	print("Hello WOrld")
	
	path_2018 = data_path + "2018_data/"

	today = "20180102"
	data_20180102 = path_2018 + today + ".csv"

	conn = sqlite3.connect("./sql_stock.db")
	cur = conn.cursor()
	
	#sel_qry = "select * from code_dic where id = ?"
	#cur.execute(sel_qry, ("004989", ))
	#rows = cur.fetchall()
	#print(type(rows))
	#print(len(rows))
	

	for l in fileToLines(data_20180102)[1:] : 
		
		ll = l.split(",")
		print("code : %s, name : %s" % (ll[1], ll[2]))

		sel_qry = "select * from code_dic where id = ?"
		cur.execute(sel_qry, (ll[1].strip(),))
		rows = cur.fetchall()
		
		if len(rows) <= 0:
			print("insert!")
			insert_qry = "insert into code_dic (id, name) values (?, ?)"
			cur.execute(insert_qry, (ll[1].strip(), ll[2].strip()))

		create_table_qry = "create table if not exists "
		table_structure =	" (\
								date TEXT not null primary key,		\
								cur_val	integer not null, \
								diff_val integer not null, \
								diff_percent, \
								trade_amount, \
								trade_money, \
								start_val, \
								high_val, \
								low_val, \
								stock_total_value, \
								stock_total_value_percent, \
								stock_paper_count, \
								stock_foreign, \
								stock_foreign_percent)"
		create_table_qry = create_table_qry + ll[1].strip() + table_structure
		print(create_table_qry)
		cur.execute(create_table_qry)

		break;

	conn.commit()
	conn.close()
		
	