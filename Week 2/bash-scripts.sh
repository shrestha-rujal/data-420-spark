#! /bin/bash

chmod +x bash-script.sh
./bash-script.sh

ls 
ls -al
ls -l
ls -a

mkdir foo
mkdir -p foo/bam/bar

cd foo/bam/bar

touch index.txt
echo "Hello world!" > index.txt

cat index.txt

rm index.txt
cd ../../../

rm -rf foo/bam

rm -rf foo


# Using variables
NAME="Rujal"

echo "My name is ${NAME}"


# Taking user input

read -p "Enter your name: " NAME

echo "Entered user name is: $NAME!" 


# if else statement
FOO=16

if [ $FOO -gt 20 ]
then
	echo "Greater than 20"
elif [ $FOO -gt 15 ]
then
	echo "Greater than 15"
else
	echo "Less than 15"	
fi


# if-else for string
read -p "Enter M/F gender: " GENDER

if [ $GENDER == "M" ]
then
	echo "Hello Sir!"
elif [ $GENDER == "F" ]
then
	echo "Hello Mam!"
else
	echo "Hello!"
fi

# File conditions

FILE="test.txt"
if [ -f "$FILE" ]
then
	echo "$FILE is a file"
else
	echo "$FILE is not a file"
fi

# Case Statement

#read -p "Are you 21 or over? Y/N or es/No " ANSWER

case "$ANSWER" in
	[yY] | [yY][eE][sS])
		echo "You're an adult"
		;;
	[nN] | [nN][oO])
		echo "You're young"
		;;
	*)
		echo "Invalid answer"
		;;
esac


#For loop
NAMES="Dave Wade Tommy Timbukche"

for NAME in $NAMES
do
	echo "Hello $NAME"
done

#For loop to rename files

FILES=$(ls *.txt)
ASSIGNMENT="assignment"

for FILE in $FILES
do
#	echo "Renaming $FILE TO ASSIGNMENT-$FILE"
	mv $FILE "$ASSIGNMENT-$FILE"
done

# While loop

LINE=1
while read -r CURRENT_LINE
do
	echo "$LINE: $CURRENT_LINE"
	((LINE++))
done <"./new-1.txt"


# Functions

function sayHello() {
	echo "Hello World!"
}

sayHello


# Function with positional parameters

function greet() {
	echo "Hello, I am $1 and I am $2"
}

greet "Rujal" 25


