from pyspark.sql import functions as F

src = (
    df_customers
    .filter(~((F.col("credit_limit") > 5500) & ~F.col("is_active")))
    .withColumn("is_jp", F.when(F.col("country") == "JP", 1).otherwise(0))
)

segment_summary = (
    src
    .groupBy("segment")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("credit_limit")).otherwise(0)).alias("singapore_credit_limit"),
        F.count("*").alias("row_count"),
    )
)

result = (
    segment_summary
    .select("segment", "singapore_credit_limit", "row_count")
    .orderBy("row_count", "segment")
)
