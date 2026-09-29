from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-06-13").cast("date"))
    .withColumn("clean_status", F.trim("status"))
)

result = (
    cleaned
    .groupBy(F.date_trunc("month", "order_date").alias("activity_month"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.avg(F.when(F.col("channel") == "partner", F.col("discount"))).alias("partner_avg_discount"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
    )
    .orderBy(F.col("distinct_customer_ids").desc_nulls_first())
)
