from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "HR", "Marketing"]))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" "), "location"))
)
