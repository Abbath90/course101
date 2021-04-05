## course101
Tasks from bigdata course101.

### hw6
Task 1:
Find top 3 most popular hotels between couples. (treat hotel as composite key of continent, country and market).

Task 2:
Find the most popular country where hotels are booked and searched from the same country. 

Task 3:
Find top 3 hotels where people with children are interested but not booked in the end

Usage:
>cd hw3/pyspark_tasks/tasks

>tasks.py --path_to_file <path_to_file> [--tasks <number_of_tasks>] [--mode <output_mode>]  - usage
>tasks.py --help - for help

--tasks - option for choosing of number of tasks. Can be 1, 2, 3 or all.
--mode - option for choosing output mode. Can be 'show' - for showing result in stdout and 'file' - for creation of file structure in './result' directory

example:

tasks.py --path_to_file C:\Users\TImur\Desktop\big_data_epam\expedia-hotel-recommendations\train.csv --task all --mode file
