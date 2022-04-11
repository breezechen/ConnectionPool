from __future__ import print_function
import sys
import contextlib

import gevent
from gevent.queue import Queue
from gevent.socket import wait_read, wait_write
from pypyodbc import connect


if sys.version_info[0] >= 3:
    integer_types = int,
else:
    import __builtin__
    integer_types = int, __builtin__.long


class DatabaseConnectionPool(object):

    def __init__(self, maxsize=100):
        if not isinstance(maxsize, integer_types):
            raise TypeError('Expected integer, got %r' % (maxsize, ))
        self.maxsize = maxsize
        self.pool = Queue()
        self.size = 0

    def get(self):
        pool = self.pool
        if self.size >= self.maxsize or pool.qsize():
            return pool.get()
        self.size += 1
        try:
            new_item = self.create_connection()
        except:
            self.size -= 1
            raise
        return new_item

    def put(self, item):
        self.pool.put(item)

    def closeall(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            with contextlib.suppress(Exception):
                conn.close()

    @contextlib.contextmanager
    def connection(self, isolation_level=None):
        conn = self.get()
        try:
            if isolation_level is not None:
                if conn.isolation_level == isolation_level:
                    isolation_level = None
                else:
                    conn.set_isolation_level(isolation_level)
            yield conn
        except:
            if not conn.connected:
                conn = None
                self.closeall()
            else:
                conn = self._rollback(conn)
            raise
        else:
            if not conn.connected:
                raise OperationalError("Cannot commit because connection was closed: %r" % (conn, ))
            conn.commit()
        finally:
            if conn is not None and conn.connected:
                if isolation_level is not None:
                    conn.set_isolation_level(isolation_level)
                self.put(conn)

    @contextlib.contextmanager
    def cursor(self, *args, **kwargs):
        isolation_level = kwargs.pop('isolation_level', None)
        with self.connection(isolation_level) as conn:
            yield conn.cursor(*args, **kwargs)

    def _rollback(self, conn):
        try:
            conn.rollback()
        except:
            gevent.get_hub().handle_error(conn, *sys.exc_info())
            return
        return conn

    def execute(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.rowcount

    def fetchone(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.fetchone()

    def fetchall(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.fetchall()

    def fetchiter(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            while True:
                items = cursor.fetchmany()
                if not items:
                    break
                yield from items


class ODBCConnectionPool(DatabaseConnectionPool):

    def __init__(self, *args, **kwargs):
        self.connect = kwargs.pop('connect', connect)
        maxsize = kwargs.pop('maxsize', 100)
        self.args = args
        self.kwargs = kwargs
        DatabaseConnectionPool.__init__(self, maxsize)

    def create_connection(self):
        return self.connect(*self.args, **self.kwargs)


if __name__ == '__main__':
    import time
    import sqlite3
    connection_str = 'DRIVER={SQL Server};SERVER=127.0.0.1;DATABASE=test;UID=sa;PWD=123456'
    sql = 'select * from test.dbo.table1'
    pool = ODBCConnectionPool(db)
    start = time.time()
    times = 1000
    for _ in range(times):
        gevent.spawn(pool.execute, sql)
    gevent.wait()
    delay = time.time() - start
    print('Running test with ConnectionPool, seconds: %.fs' % delay)

    import pypyodbc

    def test():
        pypyodbc.connect(db).cursor().execute(sql)

    start = time.time()
    for _ in range(times):
        gevent.spawn(test)
    gevent.wait()
    delay = time.time() - start
    print('Running test with odbc, seconds: %.fs' % delay)