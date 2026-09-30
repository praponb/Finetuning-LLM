from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("budget") >= 2156000))
    .withColumn("budget_whole", F.ceil("budget"))
    .withColumn("budget_int", F.col("budget").cast("int"))
)

result = (
    valid
    .groupBy("location")
    .pivot("dept_name", ["Engineering", "Sales"])
    .agg(F.avg("budget_whole").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
