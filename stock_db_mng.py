import os
import codecs
from datetime import timedelta
import datetime
import sqlite3
#import pandas as pd
import csv

data_path = "./stock_data/full_data/"

def fileToLines(path) :
	info = os.stat(path)
		
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

def query_insert_item(cursor, 
					  name,
						 date, 
						 cur_val, 
						 diff_val, 
						 diff_percent, 
						 trade_amount, 
						 trade_money, 
						 start_val, 
						 high_val, 
						 low_val, 
						 stock_total_value,
						 stock_total_value_percent,
						 stock_paper_count,
						 stock_foreign,
						 stock_foreign_percent):

	create_table_qry = "create table if not exists \""
	table_structure =	"\" (\
								date text primary key,		\
								cur_val	integer not null, \
								diff_val integer not null, \
								diff_percent real not null, \
								trade_amount integer not null, \
								trade_money integer not null, \
								start_val integer not null, \
								high_val integer not null, \
								low_val integer not null, \
								stock_total_value integer not null, \
								stock_total_value_percent real not null, \
								stock_paper_count integer not null, \
								stock_foreign integer not null, \
								stock_foreign_percent real not null)"
	create_table_qry = create_table_qry + name + table_structure
	#print(create_table_qry)
	cursor.execute(create_table_qry)

	insert_qry = "insert into \"" + name + "\" (date, cur_val, diff_val, diff_percent, trade_amount, trade_money, \
												start_val, high_val, low_val, stock_total_value, stock_total_value_percent, \
												stock_paper_count, stock_foreign, stock_foreign_percent) \
												values \
												(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	cursor.execute(insert_qry, (date, cur_val, diff_val, diff_percent, trade_amount, trade_money, 
								start_val, high_val, low_val, stock_total_value, stock_total_value_percent, 
								stock_paper_count, stock_foreign, stock_foreign_percent))
	#print(insert_qry)
	#conn.commit()

def run(path):
	
	conn = sqlite3.connect("./sql_stock.db")
	cur = conn.cursor()
	
	code_create_qry = "create table if not exists code_dic (id text primary key, name text not null)"
	cur.execute(code_create_qry)
	conn.commit()

	for l in fileToLines(path)[1:] : 
		
		ll = csv.reader([l]).__next__()
		
		#print("code : %s, name : %s" % (ll[1], ll[2]))

		sel_qry = "select * from code_dic where id = \"" + ll[1].strip() + "\""
		cur.execute(sel_qry)
		rows = cur.fetchall()
		
		if len(rows) <= 0:
			print("insert! %s: %s %s" % (path, ll[1], ll[2]))
			insert_qry = "insert into code_dic (id, name) values (?, ?)"
			cur.execute(insert_qry, (ll[1].strip(), ll[2].strip()))
			conn.commit()

		if len(ll) <= 14 :
			ll.append(0)
			ll.append(0)
			print("foreign except case : " + ll[1].strip())
		query_insert_item(cur, ll[1].strip(), today, ll[3], ll[4], ll[5], 
						   ll[6], ll[7], ll[8], ll[9], ll[10], 
						   ll[11], ll[12], ll[13], ll[14], ll[15])
		#print("query val : ")
		#print(ll)
	
	conn.commit()
	conn.close()

def drop(path):
	conn = sqlite3.connect("./sql_stock.db")
	cur = conn.cursor()
	
	for l in fileToLines(path)[1:] :
		ll = csv.reader([l]).__next__()
		drop_qry = "drop table if exists \"" + ll[1].strip() + "\""
		cur.execute(drop_qry)

	drop_qry = "drop table code_dic"
	cur.execute(drop_qry)

	conn.commit()
	conn.close()


if __name__ == "__main__" :
	
	print("Hello WOrld")
		
	oneday = timedelta(days = 1)
	dt = datetime.date(2018, 7, 9)
	#dt = datetime.date(2018, 4, 17)
	dt_dst = datetime.date(2007, 12, 31)

	while dt_dst != dt :
		
		today = dt.strftime("%Y%m%d")
		data_file = data_path + today + ".csv"

		print(data_file)

		#drop(data_file)
		#break

		run(data_file)
		dt = dt - oneday;
		#break

		

		
	