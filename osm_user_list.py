import xml.etree.cElementTree as ElementTree
import bz2
import gc


def parse(filename):
    def _connect():
        import psycopg2
        
        conn = psycopg2.connect("dbname='osmusers' host='localhost' user='osm' password='osm' options='-c log_min_messages=PANIC'")
        
        return conn
    
    def _get_information(name, attribute):
        if 'user' in attribute:
            username = attribute['user']
        else:
            username = 'anonymous'
        if 'uid' in attribute:
            uid = attribute['uid']
        else:
            uid = 0
        
        return uid, username
    
    connect = _connect()

    f = bz2.BZ2File(filename,'r')
    counter = 0
    data = iter(ElementTree.iterparse(f, events=('start', 'end')))

    event, root = data.next()

    for event, elem in data:
        if event == 'start' and elem.tag in ('node', 'way', 'relation'):
            uid, username = _get_information(elem.tag, elem.attrib)

            cursor = connect.cursor()
            try:
                cursor.execute("""INSERT INTO users (userid, username) VALUES (%s, %s);""", (uid, username))
                connect.commit()
            except:
                connect.rollback()
            cursor.close()
            del cursor

        if event == 'end':
            root.clear()
