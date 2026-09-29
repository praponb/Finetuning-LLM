from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("activity_month", F.date_trunc("MONTH", "order_date"))
    .groupingSets(
        [["status", "activity_month"], ["status"]],
        "status",
        "activity_month",
    )
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.min("discount").alias("min_discount"),
        F.count("*").alias("cnt"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("amount_per_store"),
        F.grouping("status").alias("grouping_id"),
    )
    .filter(F.col("returned_rows") >= 1)
    .orderBy(F.col("returned_rows").asc_nulls_last())
)
