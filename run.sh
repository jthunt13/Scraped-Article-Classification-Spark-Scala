# record the current directory
startWD="$PWD"
#startWD=printf "%q\n" "$(pwd)" | pbcopy

#cd $SPARK_HOME
printf "\nRunning the script through Spark...\n "
# submit the spark script
$SPARK_HOME/bin/spark-submit \
    --class wordPipeline\
    --master local[4]\
    "$PWD/target/scala-2.11/wordpipeline_2.11-1.0.jar" \
    "$PWD/data" \


# go to folder where results were put
cd $startWD/data/output

# copy and rename file of interest
touch results.csv
# cat all csv's to results csv
for filename in ./*.csv; do
    cat $filename >> results.csv
done
# move results folder back and delete output folder
mv ./results.csv ..
cd ..
rm -r ./output

#python3 ../src/dataPlotting/makePlot.py

printf "\nDone\n "
