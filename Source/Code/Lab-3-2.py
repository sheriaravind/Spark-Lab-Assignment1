import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession

os.environ["SPARK_HOME"] = r"V:\Softwares\Spark\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]= r"V:\Softwares\Spark"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#Creating the data frame and loading the data where the header is considered as the column values and to perform the queries.
df = spark.read.format("csv").option("header","true").load(r'A:\Summer -18\Big Data\Spark\lab-3\WorldCupMatches.csv')
df.show()
"""
Creating the Spark RDD's
"""

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile(r'A:\Summer -18\Big Data\Spark\lab-3\WorldcupMatches.txt')
parts = lines.map(lambda l: l.split(","))

# Each line is converted to a tuple.
matches = parts.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19] .strip()))

# The schema is encoded in a string.
schemaString = "Year Datetime Stage Stadium City Home_Team_Name Home_Team_Goals Away_Team_Goals Away_Team_Name Win_conditions Attendance Half-time_Home_Goals Half-time_Away_Goals Referee Assistant_1 Assistant_2 RoundID MatchID Home_Team_Initials Away_Team_Initials"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD
schemaTable = spark.createDataFrame(matches, schema)

# Creates a temporary view using the DataFrame
schemaTable.createOrReplaceTempView("RDDtable1")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT * FROM RDDtable1")
print("RDD Table full")
results.show()

"""
End of RDD
"""
df.createOrReplaceTempView("table1")
print("Dataframe full select Query")
data = spark.sql("select * from table1")
data.show()
# Filtering the Home teams that scored goals >2
print(r"Filtering the Home teams that scored goals >2 in Dataframe")
Goals = spark.sql("SELECT table1.Stage,table1.Stadium,table1.City,table1.HomeTeamName FROM table1 where table1.AwayTeamGoals>2")
Goals.show()

print(r"Filtering the Home teams that scored goals >2 in RDD's")
Goals_RDD = spark.sql("SELECT Stage,Stadium,City,Home_Team_Name FROM RDDtable1 where Away_Team_Goals>2")
Goals_RDD.show()

df.createOrReplaceTempView("tableA")
df.createOrReplaceTempView("tableB")
schemaTable.createOrReplaceTempView("RDDtableA")
schemaTable.createOrReplaceTempView("RDDtableB")
#Correlated Sub Query
print("Correlated SubQuery Dataframes")
Max_Attendance = spark.sql("SELECT A.Stage,A.Stadium,A.City,A.HomeTeamName,A.AwayTeamName,A.Attendance,(SELECT MAX(Attendance)FROM tableB B where A.Stadium = B.Stadium) max_attendance FROM tableA A ORDER BY max_attendance asc")
Max_Attendance.show()

print("Correlated SubQuery Spark RDD")
Max_Attendance_RDD = spark.sql("SELECT A.Stage,A.Stadium,A.City,A.Home_Team_Name,A.Away_Team_Name,A.Attendance,(SELECT MAX(Attendance)FROM RDDtableB B where A.Stadium = B.Stadium) max_attendance FROM RDDtableA A ORDER BY max_attendance asc")
Max_Attendance_RDD.show()

# Left outer join query
print("Left Outer Join DataFrame")
left_outer_join = spark.sql("SELECT A.Stadium,A.City,HomeTeamName,A.AwayTeamName,A.Attendance,B.max_attendance FRom tableA A LEFT OUTER JOIN (SELECT Stadium,MAX(Attendance) max_attendance FROM tableB B GROUP BY Stadium) B ON B.Stadium = A.Stadium ORDER BY max_attendance")
left_outer_join.show()

print("Left Outer Join Spark RDD")
left_outer_join_RDD = spark.sql("SELECT A.Stadium,A.City,Home_Team_Name,A.Away_Team_Name,A.Attendance,B.max_attendance FRom RDDtableA A LEFT OUTER JOIN (SELECT Stadium,MAX(Attendance) max_attendance FROM RDDtableB B GROUP BY Stadium) B ON B.Stadium = A.Stadium ORDER BY max_attendance")
left_outer_join_RDD.show()

# Pattern recognization query for team that reached Finals or Semi Finals
print("Pattern recognization query for team that reached Finals DataFrame")
Pattern_reg = spark.sql("SELECT * from table1 WHERE Stage LIKE 'Final' AND table1.HomeTeamName like 'Argentina'")
Pattern_reg.show()

print("Pattern recognization query for team that reached Finals Spark RDD")
Pattern_reg_RDD = spark.sql("SELECT * from RDDtable1 WHERE Stage LIKE 'Final' AND Home_Team_Name like 'Argentina'")
Pattern_reg_RDD.show()

#Right join on thw tables
print("Right Join DataFrame")
Right_Join = spark.sql("SELECT tableA.Year,tableA.Stage,tableB.AwayTeamName,tableB.HomeTeamInitials from tableA RIGHT JOIN tableB ON tableA.Datetime = tableB.Datetime")
Right_Join.show()

print("Right Join Spark RDD")
Right_Join_RDD = spark.sql("SELECT RDDtableA.Year,RDDtableA.Stage,RDDtableB.Away_Team_Name,RDDtableB.Home_Team_Initials from RDDtableA RIGHT JOIN RDDtableB ON RDDtableA.Datetime = RDDtableB.Datetime")
Right_Join_RDD.show()

#Exixts of a sub Query
print("Exists Query in Dataframes")
Exists_Query = spark.sql("SELECT tableA.Year,tableA.Stage,tableA.AwayTeamName,tableA.Stadium from tableA where exists (SELECT  tableB.HomeTeamName from tableB where tableB.Stadium = tableA.Stadium and tableB.HomeTeamGoals > 2)")
Exists_Query.show()

print("Exists Query in Spark RDD")
Exists_Query_RDD = spark.sql("SELECT RDDtableA.Year,RDDtableA.Stage,RDDtableA.Away_Team_Name,RDDtableA.Stadium from RDDtableA where exists (SELECT  RDDtableB.Home_Team_Name from RDDtableB where RDDtableB.Stadium = RDDtableA.Stadium and RDDtableB.Home_Team_Goals > 2)")
Exists_Query_RDD.show()

#Converting the Away Team Name to Upper Case
print("Upper Case in Dataframes")
Upper_Case_Query = spark.sql("SELECT UPPER(AwayTeamName) as UpperAwayTeamName FROM tableA")
Upper_Case_Query.show()

print("Upper Case in Spark RDD")
Upper_Case_Query_RDD = spark.sql("SELECT UPPER(Away_Team_Name) as UpperAwayTeamName FROM RDDtableA")
Upper_Case_Query_RDD.show()

# Average goals scored by a team
print("Average Goals Scored by a Team Dataframe")
Avg_goals = spark.sql("SELECT HomeTeamName AS Team, ROUND(AVG(HomeTeamGoals),0) AS average_goals FROM table1 GROUP BY HomeTeamName")
Avg_goals.show()

print("Average Goals Scored by a Team Spark RDD")
Avg_goals_RDD = spark.sql("SELECT Home_Team_Name AS Team, ROUND(AVG(Home_Team_Goals),0) AS average_goals FROM RDDtable1 GROUP BY Home_Team_Name")
Avg_goals_RDD.show()

#Distinct Values
print("Distinct count of Home Team Names Dataframe")
Distinct_Count_Query = spark.sql("SELECT DISTINCT count(HomeTeamName) from tableA")
Distinct_Count_Query.show()

print("Distinct count of Home Team Names Spark RDD")
Distinct_Count_Query_RDD = spark.sql("SELECT DISTINCT count(Home_Team_Name) from RDDtableA")
Distinct_Count_Query_RDD.show()

# Union Query for two tables
print("Union Query for two tables DataFrame")
Union_Query = spark.sql("SELECT * FROM tableA UNION ALL SELECT * from tableB ORDER BY tableA.HomeTeamName")
Union_Query.show()

print("Union Query for two tables Spark RDD")
Union_Query_RDD = spark.sql("SELECT * FROM RDDtableA UNION ALL SELECT * from RDDtableB ORDER BY RDDtableA.Home_Team_Name")
Union_Query_RDD.show()