from pyspark.sql import functions as F

unpivoted = (
    df_orders
    .select(
        "id",
        "store_id",
        F.col("amount").alias("AMOUNT_VAL"),
        "discount",
        "shipping_fee",
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["AMOUNT_VAL", "discount", "shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)

result = (
    unpivoted
    .cube(
        F.when(F.col("reading") >= 69, "PREMIUM").otherwise("STANDARD").alias("reading_tier"),
        "metric_name",
    )
    .agg(
        F.round(F.avg("reading"), 2).alias("avg_reading"),
        F.round(F.sum("reading") / F.nullif(F.count(F.when(F.col("metric_name") == "shipping_fee", 1)), F.lit(0)), 2).alias("reading_per_shipping_fee"),
        F.count_if(F.col("metric_name") == "discount").alias("discount_events"),
    )
)
