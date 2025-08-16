# Prints the current working directory

pwd

# List the files in your current directory, use command lines arguments to change the formatting

ls
ls -al
ls -1

# Move from one directory to another directory

cd .
pwd; ls -al
cd ..
pwd; ls -al
cd /
pwd; ls -al
cd ~
pwd; ls -al

# Create, rename, move, or delete directories

mkdir temp
mkdir -p something/nested
ls -al
ls -al something
mv temp temporary
ls -al
mv something/nested ./
ls -al
ls -al something
rm -r temporary something nested
ls -al

# Create, print, rename, move, or delete files

touch empty.csv
echo "hello world" > hello.csv
ls -al
cat empty.csv
cat hello.csv
mv hello.csv world.csv
ls -al
cp world.csv copy.csv
ls -al
cat *.csv
rm empty.csv world.csv copy.csv

# Store strings in bash variables that can be reused across multiple commands

NAME=abc123

echo $NAME
mkdir -p $NAME/data
ls -al
ls -al $NAME
ls -al $NAME/data/
echo "hello world" > $NAME/data/hello.csv
ls -al $NAME/data/
cat $NAME/data/*.csv