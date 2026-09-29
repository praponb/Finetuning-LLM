from pyspark.sql import functions as F

metrics_long = (
    df_orders
    .select("id", "store_id", "discount", "amount", "shipping_fee")
    .unpivot(
        ids=["id", "store_id"],
        values=["discount", "amount", "shipping_fee"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

result = (
    metrics_long
    .groupBy(
        F.when(F.col("value") >= 85, "LARGE").otherwise("SMALL").alias("value_tier"),
    )
    .agg(
        F.percentile_approx("value", [0.25, 0.5, 0.75]).alias("value_quartiles"),
        F.percentile_approx("value", 0.95).alias("value_p95"),
        F.count_if(F.col("kpi") == "shipping_fee").alias("shipping_fee_events"),
        F.count_if(F.col("kpi") == "discount").alias("discount_events"),
    )
    .orderBy(F.col("value_tier").desc_nulls_last())
)
