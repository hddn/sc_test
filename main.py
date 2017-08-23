import csv
import glob
import sqlite3
import time
from multiprocessing import Pool, Manager, cpu_count


object_types = {
    1: 'env',
    2: 'farm',
    3: 'farm_role',
    4: 'server'
}


def read_data(filename, q):
    """Read data from files and put rows to the Queue"""
    with open(filename, 'r') as data:
        print('Processing file: {}'.format(filename))
        start = time.time()
        reader = csv.DictReader(data)
        for row in reader:
            if row['user:scalr-meta']:
                ids = row['user:scalr-meta'].split(':')
                for key, object_type in object_types.items():
                    if ids[0] == 'v1' and ids[key]:
                        q.put((object_type, ids[key], row['Cost']))
        print('It took: {}s to process {}'.format((time.time() - start), filename))


def save_data(q, result):
    """Get rows from Queue and save results to the database"""
    while True:
        r = q.get()
        if r == 'stop':
            break
        result[r[0]][r[1]] = result[r[0]].get(r[1], 0) + float(r[2])
    conn = connect_db()
    conn.isolation_level = None
    print('Writing to db')
    start = time.time()
    for key, val in result.items():
        conn.execute('BEGIN')
        for k, v in val.items():
            conn.execute('INSERT INTO results(object_type, object_id, cost) VALUES (?, ?, ?)', (key, k, v))
        conn.execute('COMMIT')
    print('Save to db took: {}'.format(time.time() - start))
    conn.close()


def main():
    res = {val: {} for val in object_types.values()}
    manager = Manager()
    q = manager.Queue()
    pool = Pool(cpu_count() + 2)

    saver = pool.apply_async(save_data, (q, res))

    readers = []
    for i in glob.glob('*.csv'):
        reader = pool.apply_async(read_data, (i, q))
        readers.append(reader)

    for reader in readers:
        reader.get()

    q.put('stop')
    pool.close()
    pool.join()


def connect_db():
    """Return database connector"""
    return sqlite3.connect('result.db')


def create_db():
    """Create tables for object_types and result"""
    conn = connect_db()
    conn.execute('''CREATE TABLE IF NOT EXISTS types(
                    object_type TEXT PRIMARY KEY NOT NULL,
                    object_id INTEGER);''')

    for obj_id, obj_type in enumerate(object_types.values(), 1):
        conn.execute('''INSERT OR IGNORE INTO types VALUES (?, ?)''', (obj_type, obj_id))

    conn.execute('''CREATE TABLE IF NOT EXISTS results(
                    result_id INTEGER PRIMARY KEY NOT NULL,
                    object_type NOT NULL REFERENCES types(object_type),
                    object_id VARCHAR(32),
                    cost FLOAT);''')
    conn.commit()
    conn.close()


if __name__ == '__main__':
    create_db()
    main()
