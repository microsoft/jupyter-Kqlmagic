import json

class KqlRow(object):
    def __init__(self, row, col_num):
        self.row = row
        self.next = 0
        self.last = col_num


    def __iter__(self):
        self.next = 0
        return self


    def next(self):
        return self.__next__()


    def __next__(self):
        if self.next >= self.last:
            raise StopIteration
        else:
            val = self.__getitem__(self.next)
            self.next = self.next + 1
            return val


    def __getitem__(self, key):
        return self.row[key]


    def __len__(self):
        return self.last


    def __eq__(self, other):
        if (len(other) != self.last):
            return False
        for i in range(self.last):
            s = self.__getitem__(i)
            o = other[i]
            if o != s:
                return False
        return True


    def __str__(self):
        return ", ".join(str(self.__getitem__(i)) for i in range(self.last))


class KqlRowsIter(object):
    """ Iterator over returned rows, limited by size """
    def __init__(self, response, row_num, col_num):
        self.response = response
        self.next = 0
        self.last = row_num
        self.col_num = col_num


    def __iter__(self):
        self.next = 0
        self.iter_all_iter = self.response.iter_all()
        return self


    def next(self):
        return self.__next__()


    def __next__(self):
        if self.next >= self.last:
            raise StopIteration
        else:
            self.next = self.next + 1
            return KqlRow(self.iter_all_iter.__next__(), self.col_num)


    def __len__(self):
        return self.last


class KqlResponse(object):
    # Object constructor
    def __init__(self, response):
        self.data_table = None
        self.visualization_properties = None
        self.row_count = 0
        self.col_count = 0
        self.response = response
        self._get_data_table()
        self._get_visualization_properties()
        if self.data_table:
            self.row_count = len(self.data_table['Rows'])
            self.col_count = len(self.data_table['Columns'])


    def _get_data_table(self):
        if not self.data_table:
            if isinstance(self.response.get_raw_response(), list):
                for t in self.response.get_raw_response():
                    if t['FrameType'] == 'DataTable' and t['TableName'] == 'PrimaryResult':
                        self.data_table = t
                        break
            else:
                self.data_table = self.response.get_raw_response()['Tables'][0]
        return self.data_table

    def fetchall(self):
        return KqlRowsIter(self.response, self.row_count, self.col_count)


    def fetchmany(self, size):
        return KqlRowsIter(self.response, min(size, self.row_count), self.col_count)


    def rowcount(self):
        return self.row_count

    def colcount(self):
        return self.col_count

    def recordscount(self):
        return self.row_count


    def keys(self):
        result = []
        if self.data_table:
            for value in self.data_table['Columns']:
                result.append(value['ColumnName'])
        return result



    def _get_visualization_properties(self):
        " returns the table that contains the extended properties"
        if not self.visualization_properties:
            if isinstance(self.response.get_raw_response(), list):
                for t in self.response.get_raw_response():
                    if t['FrameType'] == 'DataTable' and t['TableName'] == '@ExtendedProperties':
                        for r in t['Rows']:
                            if r[1] == 'Visualization':
                                # print('visualization_properties: {}'.format(r[2]))
                                self.visualization_properties = json.loads(r[2])
            else:
                table_num = self.response.get_raw_response()['Tables'].__len__()
                for r in self.response.get_raw_response()['Tables'][table_num - 1]['Rows']:
                    if r[2] == "@ExtendedProperties":
                        t = self.response.get_raw_response()['Tables'][r[0]]
                        # print('visualization_properties: {}'.format(t['Rows'][0][0]))
                        self.visualization_properties = json.loads(t['Rows'][0][0])
        return self.visualization_properties

    def visualization_property(self, name):
        " returns value of attribute: Visualization, Title, Accumulate, IsQuerySorted, Kind, Annotation, By"
        if not self.visualization_properties:
            return None
        try:
            value = self.visualization_properties[name]
            return value if value != "" else None
        except:
            return None


    def returns_rows(self):
        return self.row_count > 0


class FakeResultProxy(object):
    """A fake class that pretends to behave like the ResultProxy.
    """
    # Object constructor
    def __init__(self, cursor, headers):
        self.fetchall = cursor.fetchall
        self.fetchmany = cursor.fetchmany
        self.rowcount = cursor.rowcount
        self.keys = lambda: headers
        self.returns_rows = True
