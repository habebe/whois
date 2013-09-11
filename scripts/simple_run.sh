java -jar build/bootstrap.jar -overwrite
java -jar build/benchmark.jar -verbose 2 -op_file ./dataset/simple.data -tx_size 1000 -tx_type pipeline
$IG_HOME/bin/igpipelineagent -loggingProperties properties/logging.properties -userTaskDirectory pipeline/ -bootfile ./data/whois.boot -monitor -idletime 500 >& agent.log



