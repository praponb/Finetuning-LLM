from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L3", "L1", "L4"]))
    .withColumn("hire_date_label", F.date_format("hire_date", "yyyy-MM"))
)
