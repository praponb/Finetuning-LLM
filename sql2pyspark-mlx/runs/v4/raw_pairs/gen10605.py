from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_rounded", F.round("budget", 2))
    .withColumn("dept_name_upper", F.upper("dept_name"))
)

result = (
    cleaned
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "Berlin"])
    .agg(F.max("budget").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "dept_name",
        F.col("Chiang Mai_amount").alias("chiang_mai_amount"),
        F.col("Chiang Mai_item_count").alias("chiang_mai_item_count"),
        F.col("Berlin_amount").alias("berlin_amount"),
        F.col("Berlin_item_count").alias("berlin_item_count"),
    )
)
