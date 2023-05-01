#!/bin/sh

for d in */ ; do

	cd $d

	sh runParser.sh > /dev/null

	rm -f autograder_db

	sqlite3 autograder_db < create.sql

	sqlite3 autograder_db < load.txt

	query_answers=( 13422 80 8365 1046871451 3130 6717 150 )
	points_per_query=6
	correct_queries=0

	for i in {1..7}
	do
		query_result=$(sqlite3 autograder_db -noheader < query$i.sql)
		if [ $query_result = ${query_answers[$i-1]} ]
		then
			((correct_queries++))
			echo "correct: correct_queries = ${correct_queries}"
		else
			c="Incorrect. Your result is ${query_result}, but it should be ${query_answers[$i-1]}."
			echo "${c}"
		fi
	done

	rm -f autograder_db

	printf '%q\n' "${PWD##*/} $((correct_queries * points_per_query))"
	echo ""
	echo ""

	cd ..

done
