START=$(date +%s)
python bench_cassandra.py $1 $2
END=$(date +%s)
DIFF=$(echo "$END - $START" | bc)
echo $DIFF
