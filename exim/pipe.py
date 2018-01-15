import psycopg2

h2 = psycopg2.connect("dbname=./edm user=edm password=edmedm port=5435 host=localhost")
pg = psycopg2.connect("dbname=edmdb user=edm password=edmedm port=5432 host=localhost")




def pipe(tables):
    source_table = tables[0]
    target_table = tables[1]
    h2c = h2.cursor()
    h2c.execute("select * from "+source_table)
    insert_statement,indexes = make_insert(h2c.description,target_table)
    if len(indexes)>0:
        for data in h2c:
            values = list()
            for index in indexes:
                values.append(data[index])
            pgc=pg.cursor()
            pgc.execute(insert_statement,values)
        pg.commit()

    h2c.close()



def make_insert(query_description, target_table):
    pgc = pg.cursor()
    pgc.execute("select * from "+target_table+" limit 0 ")
    columns = list()
    placeholders = list()
    indexes = list()
    index = -1
    for src in query_description:
        index = index + 1
        for dst in pgc.description:
            if src.name.upper() == dst.name.upper():
                indexes.append(index)
                columns.append(dst.name)
                placeholders.append("%s")

    if len(indexes) > 0:
        return "insert into "+target_table+"(" + \
                   ",".join(columns) + \
                   ") values ("+ \
                   ",".join(placeholders) + \
                   ") on conflict do nothing",indexes

    else:
        return "",list()

pipe(["public.database_config","edm.database_config"])
pipe(["public.metadata","edm.metadata"])
pipe(["public.table_info","edm.table_info"])
pipe(["public.column_info","edm.column_info"])






