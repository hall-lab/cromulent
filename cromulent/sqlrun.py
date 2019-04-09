from tabulate import tabulate

def run(db, sql_fname):
    with open(sql_fname, 'r') as f:
        sql = f.read()
        c = db.cursor()
        c.execute(sql)
        headers = [field[0] for field in c.description]
        print(tabulate(c.fetchall(), headers, tablefmt="simple"))

## -- run

## -- sqlrun
