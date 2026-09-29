from pyspark.sql import functions as F

long_format = (
    df_orders
    .select(
        "id",
        "status",
        "shipping_fee",
        F.col("discount").alias("DISCOUNT"),
        F.col("amount").alias("AMOUNT_VAL"),
    )
    .unpivot(
        ids=["id", "status"],
        values=["shipping_fee", "DISCOUNT", "AMOUNT_VAL"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
)

result = (
    long_format
    .groupBy("status")
    .agg(
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.max("val").alias("max_val"),
        F.count(F.when(F.col("metric_name") == "shipping_fee", 1)).alias("shipping_fee_rows"),
        F.count(F.when(F.col("metric_name") == "AMOUNT_VAL", 1)).alias("amount_val_count"),
    )
    .orderBy("amount_val_count")
)
