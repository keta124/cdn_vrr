import psycopg2

class Possgres_sql(object):
  def __init__(self,dbname,user,host,password):
    self.dbname=dbname
    self.user=user
    self.host=host
    self.password=password
  def connect_db(self):
    info ="dbname="+self.dbname+" user="+self.user+" host="+self.host+" password="+self.password
    conn = psycopg2.connect(info)
    return conn
  def query_db(self,query):
    conn=self.connect_db()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return rows

class Query_cdn_vrr(Possgres_sql):
  def __init__ (self,dbname,user,host,password):
    Possgres_sql.__init__(self,dbname,user,host,password)

  def insert_state_vrr(self,table,rows):
    conn=self.connect_db()
    cur = conn.cursor()
    query = "INSERT INTO " +table+" VALUES (%(network)s,%(datacenter)s,%(weight)s,%(state)s,%(timestamp)s,%(customer_isp)s)"
    for row in rows :
      cur.execute(query,row)
    conn.commit()
    conn.close()

  def insert_table_dc(self,table,rows):
    conn=self.connect_db()
    cur = conn.cursor()
    query = "INSERT INTO " +table+" VALUES (%(id)s,%(timestamp)s,%(network)s,%(weight)s,%(state)s,%(customer_isp)s)"
    for row in rows :
      cur.execute(query, row)
    conn.commit()
    conn.close()

  def update_state_column(self,table,condition):
    conn=self.connect_db()
    cur = conn.cursor()
    query ="UPDATE %s SET state ='False' %s" %(table,condition)
    cur.execute(query)
    conn.commit()
    conn.close()

