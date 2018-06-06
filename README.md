# LAM
Location aware SPARQL query implementation over GraphX

Arguments to supply:

**-g "/path/to/N3File.nt" -q /path/to/queryFile -o /path/to/outputFile -p 4 -m local**

where
-g is the path to the RDF file in N3 format stored in HDFS
-q is the local path to a query file
-o is the hdfs path to output
-p is the level of parallelism desired/ number of spark cores
-m is optional and needs to be set as "local" when only run locally
