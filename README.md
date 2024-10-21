# Sorting-compressed-time-series

This is the repository for Compressed-Sort algorithm. 

The evaluation code is based on IoTDB.

User Guide for building from source could be found here: https://github.com/apache/iotdb

## File Structure
+ `db/`: include the scripts and database used for evaluations
+ `datasets/`: include all the public datasets used for evaluations


## Steps

+ `compresseion performance`: all the test code for encoder performance is in `EncodeDecodeTest.java`
+ `Memtable sort performance`: all the test code for Memtable sort performance is in `MemtableSortTest.java`
+ `Compaction sort performance`: all the test code for Compaction sort performance is in `CompactionSorterTest.java`