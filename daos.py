from tornado import gen

import momoko
import string
import random

class UserDAO(object):
    def __init__(self, db):
        self.db = db

    #Helper Method for generating a random string of length 10
    def _get_random_str(self, size=10):
        return ''.join(random.choice(string.ascii_uppercase + string.digits)
                       for x in range(size))

    ## Models for APIS
    # Get Method for retrieving the user
    # Query Params - :id
    @gen.coroutine
    def get(self, id):
        sql = """
            SELECT id, name
            FROM users_user
            WHERE id=%s
        """
        cursor = yield momoko.Op(self.db.execute, sql, (id,))
        desc = cursor.description
        result = [dict(zip([col[0] for col in desc], row))
                         for row in cursor.fetchall()]

        cursor.close()
        yield result

    # Get Method for retrieving the list of users  
    @gen.coroutine
    def get_list(self):
        sql = """
            SELECT id, name
            FROM users_user
        """
        cursor = yield momoko.Op(self.db.execute, sql)
        desc = cursor.description
        result = [dict(zip([col[0] for col in desc], row))
                         for row in cursor.fetchall()]

        cursor.close()
        yield result

    # Post Method for creating users
    @gen.coroutine
    def create(self):
        sql = """
            INSERT INTO users_user (name)
            VALUES (%s)
        """
        name = self._get_random_str()
        cursor = yield momoko.Op(self.db.execute, sql, (name))
        yield cursor

    # Put Method for updating users
    @gen.coroutine
    def update(self, id, data={}):
        fields = ''
        for key in data.keys():
            fields += '{0}=%s,'.format(key)

        sql = """
            UPDATE users_user
            SET {0}
            WHERE id=%s
        """.format(fields[0:-1])
        params = list(data.values())
        params.append(id)
        cursor = yield momoko.Op(self.db.execute, sql, params)
        yield cursor

    # DeleteTable for deleting table
    @gen.coroutine
    def delete_table(self):
        sql = """
            DROP TABLE IF EXISTS users_user;
            DROP SEQUENCE IF EXISTS user_id;
        """
        cursor = yield momoko.Op(self.db.execute, sql)
        yield cursor

    # Delete Method for deleting table
    @gen.coroutine
    def delete(self, id):
        sql = """
            DELETE
            FROM users_user
            WHERE id=%s
        """
        cursor = yield momoko.Op(self.db.execute, sql, (id,))
        cursor.close()
        yield ''

    # Create Table Method - which creates a table according to hard coded schema
    @gen.coroutine
    def create_table(self, callback=None):
        sql = """
            CREATE SEQUENCE  user_id;
            CREATE TABLE IF NOT EXISTS users_user (
                id integer PRIMARY KEY DEFAULT nextval('user_id') ,
                name  varchar(80) UNIQUE
            );
            ALTER SEQUENCE user_id OWNED BY users_user.id;
        """
        cursor = yield momoko.Op(self.db.execute, sql)
        yield cursor