from pyspark.sql import functions as F

long_format = (
    df_orders
    .select("id", "store_id", "amount", "shipping_fee")
    .unpivot(["id", "store_id"], ["amount", "shipping_fee"], "kpi", "value")
    .filter(F.col("value").isNotNull())
)

result = (
    long_format
    .groupBy(
        "kpi",
        F.when(F.col("value") >= 82, "HIGH").otherwise("LOW").alias("value_tier"),
    )
    .agg(
        F.max(F.when(F.col("kpi") == "shipping_fee", F.col("value"))).alias("shipping_fee_max_value"),
        F.count_if(F.col("kpi") == "shipping_fee").alias("shipping_fee_events"),
        F.round(F.sum("value") / F.nullif(F.count(F.when(F.col("kpi") == "amount", 1)), F.lit(0)), 2).alias("value_per_amount"),
        F.count(F.when(F.col("kpi") == "shipping_fee", 1)).alias("shipping_fee_rows"),
        F.round(F.sum("value") / F.nullif(F.count(F.when(F.col("kpi") == "amount", 1)), F.lit(0)), 2).alias("value_per_amount_2"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1)
    .orderBy(F.col("shipping_fee_events").desc())
)
