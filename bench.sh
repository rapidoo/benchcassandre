python drop.py $2
for i in `seq 1 $1`; do  (./process.sh $2 $3 &) ; done
