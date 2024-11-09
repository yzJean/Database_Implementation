#! /usr/bin bash

build_dir="./build/"
if [ -z $build_dir ]; then
    mkdir -p $build_dir
fi
javac Database.java -d $build_dir
echo "Built successfully"

main_program="Database"
cd $build_dir
java $main_program
echo "End of program"