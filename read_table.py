import sqlite3

conn = sqlite3.connect("./data/jobs.db")

cursor =conn.cursor()

cursor.execute("SELECT * FROM e_jobs")

print(cursor.fetchall())