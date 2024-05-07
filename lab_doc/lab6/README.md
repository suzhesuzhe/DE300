# Lab 6 Assignment

1, Run all the cells in ./spark-sql/pyspark-sql.ipynb, grasp a good understanding of the functions in the notebook.

2, Add one more cell ./spark-sql/pyspark-sql.ipynb which transforms the pyspark dataframe into pandas dataframe and print out the summary statistics using 'describe()'.

3, Wrap all functions in the ./spark-sql/pyspark-sql.ipynb notebook into a .py file and write a run-py-spark.sh file (similar to the one in the word-counts folder). \
When you run 'bash run-py-spark.sh', the .py file should be executed with Spark (i.e the steps of data reading, data cleaning, data Transformation), and the final dataframe should be stored as .csv file in the './data' folder.
