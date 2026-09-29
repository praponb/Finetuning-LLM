from pyspark.sql import functions as F

result = (
    df_departments
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("adjusted_budget", F.round((F.col("budget") * 0.85), 2))
)
