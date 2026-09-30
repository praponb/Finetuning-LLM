from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("is_new_york", F.when(F.col("location") == "New York", 1).otherwise(0))
)

result = (
    cleaned
    .groupBy("dept_name")
    .pivot("location", ["New York", "Singapore"])
    .agg(F.max("budget").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "dept_name",
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_item_count").alias("new_york_item_count"),
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_item_count").alias("singapore_item_count"),
    )
)
