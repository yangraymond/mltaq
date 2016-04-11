import numpy as np
import pandas as pd
import zipfile
from pytz import timezone
from datetime import datetime
from bytespec import ByteSpec

# A collection of tools to process financial time-series data   

class TaqDataFrame:

    # Attributes users should be able to access

    def __init__(self, fname, type, chunksize=100, process=True):
        '''
        Initializes an empty pandas dataframe for storing tick data.
        fname: file name (and directory) of original TAQ file
        type: 'master', 'quotes', 'trades', or 'bbo'
        chunksize: number of lines to read in at once
        '''
        self.fname = fname
        assert type in ['mtr', 'qts', 'trd', 'bbo']
        self.type = type
        self.chunksize = chunksize
        self.process = process
        self.df = pd.DataFrame()

    def load(self):
        '''
        Loads in data from fname.
        '''
        with zipfile.ZipFile(self.fname) as zf:
            with zf.open(zf.namelist()[0]) as infile:
                header = infile.readline()
                datestr, record_count = header.split(b':')
                self.month = int(datestr[2:4])
                self.day = int(datestr[4:6])
                self.year = int(datestr[6:10])
                self.record_count = int(record_count)
                self.line_len = len(header)
                # Computes the time at midnight starting at date specified in spec
                # given in milliseconds since epoch
                utc_base_time = datetime(self.year, self.month, self.day)
                self.base_time = timezone('US/Eastern').\
                                                localize(utc_base_time).\
                                                timestamp()

                if self.type == 'mtr':
                    self.dtype = ByteSpec().mtr_col_dt
                elif self.type == 'qts':
                    self.dtype = ByteSpec().qts_col_dt
                elif self.type == 'trd':
                    self.dtype = ByteSpec().trd_col_dt
                else:
                    self.dtype = ByteSpec().bbo_col_dt

                # Iterate through infile by chunksize
                while True:
                    bytes = infile.read(self.chunksize*self.line_len)
                    if not bytes:
                        break
                    rows = len(bytes) // self.line_len
                    records = np.ndarray(rows, dtype=self.dtype, buffer=bytes)

                    # With smaller chunksizes, the constant reassignment of self.df may be slow
                    self.df = self.df.append(pd.DataFrame(records), ignore_index=True)
 
        assert not self.df.empty
        self.df = self.df.drop('Line_Change', axis=1)

        if self.process:
            # TODO: vectorized byte processing, make human-readable
            # print (self.df)

            # Clobber HHMMSSXXX into decimal unix time
            # numer_df = self.df.drop(ByteSpec().bbo_strings, axis=1)

            numer_df = self.df[ByteSpec().dict[self.type+'_numericals']]
            strings_df = self.df[ByteSpec().dict[self.type+'_strings']]
            
            

            # self.df['Timestamp'] = self.base_time + self.df['Hour']*3600      
            # print ()

            return None
                    

        return self

    def query(self, stock, time, volume, price):
        if self.df.empty:
            raise ValueError('Must load in tick data into TaqDataFrame first')

        return None

# Goals: have TaqDataFrame object, pass into Featurizer,