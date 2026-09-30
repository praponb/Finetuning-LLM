from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2024-10-05")
    .withColumn("full_name_filled", F.coalesce("full_name", F.lit("N/A")))
    .withColumn("level_length", F.length("level"))
)
