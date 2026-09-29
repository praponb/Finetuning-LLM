from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([16, 21, 31, 34]) | F.col("dept_id").isin([22, 25, 38]))
    .withColumn("level_title", F.initcap(F.trim("level")))
)
