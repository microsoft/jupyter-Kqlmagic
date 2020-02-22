from io import BytesIO, SEEK_SET, SEEK_END
import ijson
from .log import logger

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

import csv
import itertools

class CSV_table_reader(list):
      #a wrapper class for List that iterates over a csv file #
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

    def __init__(self, foldername):
        from .display import Display
        from .constants import Constants

        self.foldername = f"{Display.showfiles_base_path}/{Display.showfiles_folder_name}/{foldername}"
        self.buffer_size = Constants.STREAM_BUFFER_SIZE
        self.buff_start = 0
        self.current_csv_number = 1
        self.row_buffer = self.initialize_buffer()

    def initialize_buffer(self):
        import time
        now = time.time()
        with open(f"{self.foldername}/{self.current_csv_number}.csv", "r") as infile:
            r = csv.reader(infile)
            tmp_buffer = []
            for num in (itertools.islice(r, 0, self.buffer_size, None)):
                tmp_buffer.append(num) 
        after = time.time()
        print(f"init {self.buffer_size} took  {after-now} s")           
        return tmp_buffer
    def __getitem__(self, i):
        start_edge = self.buff_start
        k = (i % self.buffer_size)
        end_edge = self.buff_start + self.buffer_size        

        if i>=start_edge and i<end_edge:
            return self.row_buffer[k]
        else:
            try:
                self.current_csv_number = (i // self.buffer_size )+1
                self.row_buffer = self.initialize_buffer()
                self.buff_start =(self.current_csv_number-1) * self.buffer_size
                return self.row_buffer[k]
            except FileNotFoundError:
                raise IndexError

    def __len__(self):
        self.len = self.get_len()
        return self.len

    def get_len(self):
        i =0
        try:
            while True:
                self.__getitem__(i)
                i+=1
        except IndexError:
            return i
        return i
