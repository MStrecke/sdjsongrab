#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class Query_builder:
    """ This class helps building queries.
        It takes pairs of (fieldname, value) and outputs the string
            "field1=%s, field2=%s, field3=%s"
        and an array of the values
            [value1, value2, value3, ...]
        for use in an SQL execute
    """

    def __init__(self, table, *, joker='?'):
        self.fields = []
        self.values = []
        self.table = table
        self.joker = joker

    def add(self, field, value, override=False):
        """ add field/value pair

        :param field: field name
        :param value: value
        :param override: replace field entry with same name
        :return:
        """
        if field in self.fields:
            if override:
                po = self.fields.index(field)
                self.fields.pop(po)
                self.values.pop(po)
            else:
                raise IndexError("field name occured more than once")
        self.fields.append(field)
        self.values.append(value)

    def add_dict(self, datadict, transfer=None):
        """ add content of dict, renamed by transfer

        :param datadict: dict with "fieldname: data" to be added to the query
        :param transfer: dict with "data_fieldname: sql_fieldname" (keys not present here, will not be added)
        """
        for key in list(datadict.keys()):
            if transfer is None:
                self.add(key, datadict[key])
            else:
                nkey = transfer.get(key)
                if nkey is not None:
                    self.add(nkey, datadict[key])

    def add_dict_filter(self, datadict, filter, rename=None):
        """ add content of dict, renamed by transfer

        :param datadict: dict with "fieldname: data" to be added to the query
        :param filter: list of valid entries
        """
        for key in list(datadict.keys()):
            if rename is not None:
                # get renamed key name, if not present, use current one
                sqlkey = rename.get(key, key)
            else:
                sqlkey = key
            if sqlkey in filter:
                self.add(sqlkey, datadict[key])

    def get_query(self):
        raise NotImplementedError

    def execute(self, cursor, debug=False):
        """ execute the query

        :param cursor: provided cursor
        :return:
        """
        query, param = self.get_query()
        if debug:
            print("Query:", query)
            print("Param:", param)
        try:
            cursor.execute(query, param)
        except:
            print("query: %s\nparams: %s" % (query, param))
            raise

    def get_insert(self, addclause=None):
        """ return SQL statement to INSERT data

        .param addclause: additional SQL clause (add values to returned param array)
        """
        s = 'INSERT INTO %s (' % self.table
        s += ", ".join(self.fields)
        s += ") VALUES ("
        s += ", ".join([self.joker]*(len(self.values)))
        if addclause is None:
            addclause = ''
        s += ") " + addclause
        return s, self.values

    def execute_insert(self, cursor, addclause=None, debug=False):
        """ execute an insert with accumulated fields/params

        :param cursor: cursor to use
        :param addclause: additional clause to be appended to statement
        :return:
        """
        query, param = self.get_insert(addclause)
        if debug:
            print("Query:", query)
            print("Param:", param)
        try:
            return cursor.execute(query, param)
        except:
            print("query=%s, param=%s" % (query, param))
            raise

    def get_update(self, whereclause, whereparam=None):
        """ return SQL statement to UPDATE data set

        .param whereclause: WHERE clause of statement (add values to returned param array)
        """
        s = 'UPDATE %s SET ' % self.table
        s += ", ".join(["%s=%s" % (x, self.joker) for x in self.fields])
        s += " WHERE " + whereclause

        if whereclause is not None:
            if whereparam is not None:
                self.values.extend(whereparam)
        return s, self.values

    def execute_update(self, cursor, whereclause, whereparam=None, debug=False):
        """ execute an update with accumulated fields/params

        :param cursor: cursor to use
        :param whereclause: where clause (without WHERE)
        :param whereparam: params for where clause
        :return:
        """
        query, param = self.get_update(whereclause, whereparam)
        if debug:
            print("Query:", query)
            print("Param:", param)
        return cursor.execute(query, param)

    def insert_or_update(self, cursor, select_database_field, compare_query_field=None, debug=False):
        """ insert or update entry

        :param cursor: database cursur
        :type cursor: cursor
        :param select_database_field: name of field on which the decision insert/update is made
        :type select_database_field: str
        :param compare_query_field: name of field, update existing entry if these values don't match
        :type compare_query_field: str or None (-> no comparison)
        :param debug: print debug values, defaults to False
        :type debug: bool, optional
        :return: True: item was added or updated, False: update was skipped
        :rtype: bool
        :note: raises value error if selectfield is not in fields
        """

        selix = self.fields.index(select_database_field)
        selval = self.values[selix]

        compix = None
        compval = None
        if compare_query_field is not None:
            compix = self.fields.index(compare_query_field)
            compval = self.values[compix]

        query = "SELECT * FROM " + self.table + " WHERE " + select_database_field + "=?"
        param = (selval,)
        if debug:
            print("Query:", query)
            print("Param:", param)

        cursor.execute(query, param)
        r = cursor.fetchone()
        if debug:
            print("Result:", r)

        if r is None:
            if debug:
                print("insert")

            self.execute_insert(cursor, debug=debug)
            return True
        else:
            if compare_query_field is not None:
                oldval = r[compare_query_field]
                if oldval == compval:
                    if debug:
                        print("* Not updated, because same compare values")
                    return False

            self.execute_update(
                cursor, select_database_field + "=" + self.joker, (selval,), debug=debug)

            return True


def local_cursor_wrapper(func):
    """ decorator to ensure that the function receives a cursor to the local database

    Advantage:
      - When the function is called multiple times, the parent function can create the db cursor which the
        function will (re)used.
      - Otherwise the wrapper creates and closes a cursor automatically.

    Notes:
      - The base class MUST have a function named `cursor()` that creates a cursor to the database.
      - When the functions is called with a (reused) cursor the keyword parameter `cursor=...` must be used.
      - The parameterlist of the function MUST have a parameter named `cursor` (which is usually set to None)

    Examples:

    @local_cursor_wrapper
    def abcde(self, a, b, *, cursor=None):
       cursor.execute(....)
       ...

    # use single (reused) cursor
    cu=db.cursor()
    self.abcde(...., cursor=cu)
    self.abcde(...., cursor=cu)
    self.abcde(...., cursor=cu)
    cu.close()

    # generate (and destroy) cursor for a single call automatically
    self.abcde(....)  or
    self.abcde(...., cursor=None)

    # class must have a function to create a cursor, e.g.:
    def cursor(self):
        return con.cursor()
    """
    def function_wrapper(*args, **kwargs):
        cursor = kwargs.get('cursor')
        if cursor is not None:
            return func(*args, **kwargs)

        self = args[0]
        cu = self.cursor()
        kwargs['cursor'] = cu
        try:
            res = func(*args, **kwargs)
        except:
            raise
        finally:
            cu.close()
        return res

    return function_wrapper
