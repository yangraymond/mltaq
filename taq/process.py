import numpy as np
import pandas as pd
import zipfile
from pytz import timezone
from datetime import datetime

# A collection of tools to process financial time-series data

class ByteSpec:
    '''
    A description of the records in TAQ file. Borrowed from @davclark
    '''
    bbo_col_dt = [  ('Hour',                       'S2'),
                    ('Minute',                     'S2'),
                    ('Second',                     'S2'),
                    ('Milliseconds',               'S3'),
                    ('Exchange',                   'S1'),
                    ('Symbol_Root',                'S6'),
                    ('Symbol_Suffix',             'S10'),
                    ('Bid_Price',                 'S11'),
                    ('Bid_Size',                   'S7'),
                    ('Ask_Price',                 'S11'),
                    ('Ask_Size',                   'S7'),
                    ('Quote_Condition',            'S1'), 
                    ('Market_Maker',               'S4'),
                    ('Bid_Exchange',               'S1'),
                    ('Ask_Exchange',               'S1'),
                    ('Sequence_Number',           'S16'),
                    ('National_BBO_Ind',           'S1'),
                    ('NASDAQ_BBO_IND',             'S1'),
                    ('Quote_Cancel_Correction',    'S1'),
                    ('Source_of_Quote',            'S1'),
                    ('Retail_Interest_Ind',        'S1'),
                    ('Short_Sale_Restriction_Ind', 'S1'),
                    ('LULD_BBO_Ind_CQS',           'S1'),
                    ('LULD_BBO_Ind_UTP',           'S1'),
                    ('FINRA_ADF_MPID_Ind',         'S1'),
                    ('SIP_Generated_Message_ID',   'S1'),
                    ('National_BBO_LULD_Ind',      'S1'),
                    ('Line_Change',                'S2')  ]

    trd_col_dt = []
    qts_col_dt = []
    mtr_col_dt = []



    

class TaqDataFrame:

    def __init__(self, fname, type, chunksize=100, process=True):
        '''
        Initializes an empty pandas dataframe for storing tick data.
        fname: file name (and directory) of original TAQ file
        type: 'master', 'quotes', 'trades', or 'bbo'
        chunksize: number of lines to read in at once
        '''
        self.fname = fname
        assert type in ['master', 'quotes', 'trades', 'bbo']
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

                if self.type == 'master':
                    self.dtype = ByteSpec().mtr_col_dt
                elif self.type == 'quotes':
                    self.dtype = ByteSpec().qts_col_dt
                elif self.type == 'trades':
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
                    if self.df.empty:
                        self.df = pd.DataFrame(records)
                    else:
                        self.df = self.df.append(pd.DataFrame(records), ignore_index=True)
                    

        return self

# Goals: have TaqDataFrame object, pass into Featurizer,