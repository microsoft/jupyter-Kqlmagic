from io import BytesIO, SEEK_SET, SEEK_END
import ijson
class ResponseStream(object): #wrapper class to wrap a stream as a File object - for ijson
    def __init__(self, request_iterator):
        self._bytes = BytesIO()
        self._iterator = request_iterator

    def _load_all(self):
        self._bytes.seek(0, SEEK_END)
        for chunk in self._iterator:
            self._bytes.write(chunk)

    def _load_until(self, goal_position):
        current_position = self._bytes.seek(0, SEEK_END)
        while current_position < goal_position:
            try:
                current_position = self._bytes.write(next(self._iterator))
            except StopIteration:
                break

    def tell(self):
        return self._bytes.tell()

    def read(self, size=None):
        left_off_at = self._bytes.tell()
        if size is None:
            self._load_all()
        else:
            goal_position = left_off_at + size
            self._load_until(goal_position)

        self._bytes.seek(left_off_at)
        return self._bytes.read(size)

    def seek(self, position, whence=SEEK_SET):
        if whence == SEEK_END:
            self._load_all()
        else:
            self._bytes.seek(position, whence)

class CSV_table_reader(list): #a wrapper class for List that iterates over a csv file #
    map_key_to_index = {
        'FrameType':0,'TableId':1,'TableKind':2,'TableName':3,'Columns':4,'Rows':5
    }
    def __iter__(self):
        self.i = 0
        return self
    def __next__(self):
        try:
            result = self.__getitem__(self.i)
            self.i = self.i+1
            return result
        except:
            raise StopIteration

    def __init__(self, filename):
        self.filename = filename
    def __getitem__(self, i):
        import csv
        if i<0:
            n = self.__len__()
            i = n+i
        with open(self.filename+ ".csv", "r") as infile:
            r = csv.reader(infile)
            next(r) #skip headers
            try:
                for i in range(i-1):
                    next(r)     
                return next(r)    
            except StopIteration:
                raise IndexError(self.filename)
        # f = open("DataTable.csv",'r')
        # json_response = ijson.items(f, 'item')
        # for t in json_response:
        #     if cnt==i:
        #         # if t["FrameType"] == "DataTable":
        #         return t
        #     # if t["FrameType"] == "DataTable":
        #     cnt+=1

    def __len__(self):
        length = 0
        import csv
        with open(self.filename+ ".csv", "r") as infile:
            r = csv.reader(infile)
            next(r) #skip headers
            try:
                next(r)     
                length+=1
            except StopIteration:
                return length
        return length
        # f = open("all_tables.txt",'r')
        # json_response = ijson.items(f, 'item')

        # for t in json_response:
        #     # if t["FrameType"] == "DataTable":
        #     length+=1
        # return length

    def __str__(self):
        res ="["
        import csv
        with open(self.filename+ ".csv", "r") as infile:
            r = csv.reader(infile)
            try:
                while True:
                    row = next(r)   
                    res+=str(row)[1:-1]
            except StopIteration:
                return res
        return res+"]"
        # f = open("all_tables.txt",'r')
        # json_response = ijson.items(f, 'item')
        # for t in json_response:
        #     # if t["FrameType"] == "DataTable":
        #     res+=str(t)
        # return res

from .kql_proxy import KqlTableResponse

class tables_gen(list):
    def __init__(self, response):
        self.response = response
    def __getitem__(self, i):
        try:
            t = self.response.primary_results[i]
            to_return = KqlTableResponse(t, self.response.visualization_results.get(int(t.id), {}))
        except  ValueError:
            to_return = KqlTableResponse(t, self.response.visualization_results.get(t.id, {}))
        return to_return
    def __len__(self):
        return len(self.response.primary_results)

from .kql_response import KqlResponseTable_CSV
class primary_results(list):
    def __init__(self, tables):
        self.tables = tables
    
    def __getitem__(self, i):
        t = self.tables[i]
        return KqlResponseTable_CSV(t[1], t)

    def __len__(self):
        return len(self.tables)
