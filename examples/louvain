#! /bin/bash

T="$(date +%s)"
java -cp "build/dist/*" com.soteradefense.dga.graphx.louvain.Main --jars build/dist/dga-graphx-0.1.jar,build/dist/spark-graphx_2.10-0.9.0-cdh5.0.0-beta-2.jar "$@"

T="$(($(date +%s)-T))"
echo "Time in seconds: ${T}"


bin/louvain -i examples/small_edges.tsv -o test_output --edgedelimiter "\t" 2> stderr.txt

-i F:\workspace-scala\loovain\examples\small_edges.tsv -o F:\workspace-scala\loovain\examples\out --edgedelimiter "\t"


