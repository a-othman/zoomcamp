
for j in 2019 2020 2021
do
    for i in 01 02 03 04 05 06 07 08 09 10 11 12
    do  
        wget  https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_$j-$i.csv.gz
    done
done