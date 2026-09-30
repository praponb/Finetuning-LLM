from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "HR"]))
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("OTHER")))
)

result = (
    cleaned
    .groupBy("location")
    .pivot("dept_name", ["Legal", "HR"])
    .agg(F.sum("dept_name_length"))
)
