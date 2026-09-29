from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("budget") >= 3127000)
    .withColumn("budget_tier", F.when(F.col("budget") >= 3666000, "High").when(F.col("budget") >= 1855000, "Medium").otherwise("Low"))
    .withColumn("location_filled", F.coalesce("location", F.lit("UNKNOWN")))
)

result = (
    base
    .groupBy("location")
    .pivot("dept_name", ["Legal", "HR"])
    .agg(F.max("budget").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
