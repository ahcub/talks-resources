from os_utils.logging_utils import timeit, configure_stream_logger
import pandas as pd

configure_stream_logger()

rows = 10000
cols = 3000
default_data = [[1, 2, 3, 4]]
col_1 = [1] * rows
col_2 = [2] * rows
col_3 = [3] * rows
col_4 = [4] * rows
table = default_data * rows

#1 naive extension
df = pd.DataFrame()
for i in range(0, cols):
    df[(i*4)] = col_1
    df[(i*4)+1] = col_2
    df[(i*4)+2] = col_3
    df[(i*4)+3] = col_4

#2 concatenation
df = pd.DataFrame()
dfs = [df]
for i in range(0, cols):
    dfs.append(pd.DataFrame(table))
df = pd.concat(dfs, axis=1)



10k rows x 12k columns



TIME SPENT: 0:01:55.931071


TIME SPENT: 0:00:29.759407

# struct
temp_data = decompress(comp_struct)
cursor = 0
chunks = []
while cursor < len(temp_data):
    bytes_to_read, = struct.unpack('!Q', temp_data[cursor:cursor+8])
    cursor += 8
    data_chunk = temp_data[cursor:cursor+bytes_to_read]
    fmt = "<%df" % (len(data_chunk) // 4)
    chunks.append(pd.DataFrame(list(struct.unpack(fmt, data_chunk))))
    cursor += bytes_to_read
pd.concat(chunks)



# struct
temp_data = decompress(comp_struct)
fmt = "<%df" % (len(temp_data) // 4)
pd.DataFrame(list(struct.unpack(fmt, temp_data)))

#csv
pd.read_csv(BytesIO(decompress(comp_string))

# numpy bytestream
pd.DataFrame(np.frombuffer(decompress(comp_np))





def stream_data(chunk_files):
    for file_path in chunk_files:
        with open(file_path) as file:
            yield file.read()



            import numpy as np
            a = np.array([1, 2, 3])
            print(a)
            >>> [1 2 3]




a = np.array([[1, 2, 3],
              [4, 5, 6],
              [7, 8, 9]])
b = np.array([[3, 3, 3]])

print(a * b)
>>> [[ 3  6  9]
     [12 15 18]
[21 24 27]]


a = np.random.rand(100, 5)
b = np.random.rand(3, 5)
a*b # doesn't work
a = np.repeat(a[:, np.newaxis], 3, axis=1)
a*b # shape (100, 3, 5)


a = np.random.rand(100, 3, 5)
b = np.random.rand(5, 3)
a * b # doesn't work
a = np.rot90(a, 1, (1, 2)) # shape (100, 5, 3)
a * b


import pandas as pd
index = pd.date_range('2018-01-01', freq='B', periods=5)
df = pd.DataFrame(np.random.rand(5, 3), index=index)
print(df)
>>>                0         1         2
2018-01-01  0.146294  0.779070  0.524988
2018-01-02  0.312610  0.329260  0.476083
2018-01-03  0.310025  0.150830  0.827542
2018-01-04  0.196156  0.286346  0.512029
2018-01-05  0.502830  0.607060  0.391456




import pandas as pd
index = pd.date_range('2018-01-01', freq='B', periods=5)
df = pd.DataFrame(np.random.rand(5, 3), index=index)
df[4] = np.random.rand(5)
print(df)
>>>                0         1         2         4
2018-01-01  0.000620  0.990465  0.102992  0.149875
2018-01-02  0.558548  0.713990  0.045607  0.394465
2018-01-03  0.540933  0.939931  0.943576  0.541685
2018-01-04  0.973065  0.261491  0.232230  0.195933
2018-01-05  0.045850  0.982684  0.829130  0.124526



100k rows x 20k columns

16 DataFrames to concat

20k columns to add

Time: 29min


Time: 11s






>>> import pandas as pd
>>> date_range = pd.date_range('2019-01-10',
                               ...                            '2019-01-17', freq='B')
>>> print(date_range)
DatetimeIndex(['2019-01-10', '2019-01-11', '2019-01-14', '2019-01-15',
               '2019-01-16', '2019-01-17'],
              dtype='datetime64[ns]', freq='B')

>>> print(date_range + pd.Timedelta('5h'))
DatetimeIndex(['2019-01-10 05:00:00', '2019-01-11 05:00:00',
               '2019-01-14 05:00:00', '2019-01-15 05:00:00',
               '2019-01-16 05:00:00', '2019-01-17 05:00:00'],
              dtype='datetime64[ns]', freq='B')

>>> print(date_range.strftime('%Y-%m-%d').values)
['2019-01-10' '2019-01-11' '2019-01-14' '2019-01-15' '2019-01-16'
 '2019-01-17']