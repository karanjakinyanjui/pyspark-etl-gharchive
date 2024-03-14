mkdir -p github-data/input github-data/output 
for i in {0..10}
do
    wget https://data.gharchive.org/2015-01-01-$i.json.gz -P github-data/input/
done