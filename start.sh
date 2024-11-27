#!/usr/bin bash
set -e

build_dir="./build/"
if [ -z $build_dir ]; then
    mkdir -p $build_dir
fi
javac *.java -d $build_dir 
echo "Built successfully"

cd $build_dir
echo ""
echo "Run Database1: "
main_program="Database1"
java $main_program
echo "End of Database1"

echo ""
echo "Run Database2: "
main_program="Database2"
java $main_program
echo "End of Database2"
