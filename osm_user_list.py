import xml.etree.cElementTree as ElementTree
import gzip


def parse(filename):
    def _connect():
        import psycopg2
        
        conn = psycopg2.connect("dbname='osmusers' host='localhost' user='osm' password='osm'")
        
        return conn
    
    def _get_information(name, attribute):
        username = attribute['user']
        uid = attribute['uid']
        
        return uid, username
    
    connect = _connect()
    
    for event, elem in ElementTree.iterparse(gzip.open(filename, 'rb'), events=('start', 'end')):
        if event == 'start' and elem.tag in ('node', 'way', 'relation'):
            uid, username = _get_information(elem.tag, elem.attrib)

            cursor = connect.cursor()
            try:
                cursor.execute("""INSERT INTO users (userid, username) VALUES (%s, %s);""", (uid, username))
                connect.commit()
            except:
                connect.rollback()
        elem.clear()
