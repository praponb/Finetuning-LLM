from pyspark.sql import functions as F

unpivoted = (
    df_orders
    .select(
        "id",
        "store_id",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("amount").alias("AMOUNT"),
        F.col("shipping_fee").alias("SHIPPING_FEE"),
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["DISCOUNT_VAL", "AMOUNT", "SHIPPING_FEE"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("reading") >= 84, "PREMIUM").otherwise("STANDARD").alias("reading_tier"),
        "store_id",
    )
    .agg(
        F.count(F.when(F.col("metric_name") == "DISCOUNT_VAL", 1)).alias("discount_val_count"),
        F.percentile_approx("reading", [0.25, 0.5, 0.75]).alias("reading_quartiles"),
        F.sum(F.when(F.col("metric_name") == "SHIPPING_FEE", F.col("reading"))).alias("shipping_fee_total_reading"),
    )
    .orderBy(F.col("shipping_fee_total_reading").desc_nulls_last())
)
