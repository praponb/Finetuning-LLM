from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("customer_id")
    .subtract(
        df_orders
        .filter(F.col("discount") <= 0.29)
        .select("customer_id")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("activity_month", F.date_trunc("MONTH", "order_date"))
    .groupingSets(
        [["activity_month", "customer_id"], ["activity_month"]],
        "activity_month",
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count_2"),
        F.sum(F.when(F.col("channel") == "store", F.col("shipping_fee")).otherwise(0)).alias("store_shipping_fee"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("amount_per_store"),
    )
    .orderBy(F.col("amount_per_store").desc_nulls_first())
)
