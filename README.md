# LAM
Location aware SPARQL query implementation over GraphX

Arguments to supply (Local mode):

**-g "/path/to/N3File.nt" -q /path/to/queryFile -o /path/to/outputFile -p 4 -m local**

where
-g is the path to the RDF file in N3 format stored in HDFS
-q is the local path to a query file
-o is the hdfs path to output
-p is the level of parallelism desired/ number of spark cores (here set to 4)
-m is optional and needs to be set as "local" when only run locally

Arguments to supply (Cluster YARN mode):

**spark-submit --class main --master yarn --deploy-mode cluster --num-executors 6 --executor-cores 4 --files qyeryFile LAM.jar -g "/path/to/N3File.nt" -q /path/to/queryFile -o /path/to/outputFile -p 4**

--num-executors is the number of machines to be used in the cluster (here 6)
--executor-cores is the number of cores per machine, set it equal to the level of parallelism desired (here 4)