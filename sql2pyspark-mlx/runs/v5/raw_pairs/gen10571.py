from pyspark.sql import functions as F

unpivoted = (
    df_orders
    .select("id", "channel", "discount", "shipping_fee", "amount")
    .unpivot(
        ids=["id", "channel"],
        values=["discount", "shipping_fee", "amount"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("val") >= 87, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("metric") == "amount", 1)).alias("amount_count"),
        F.sum(F.when(F.col("metric") == "discount", F.col("val")).otherwise(0)).alias("discount_val"),
        F.size(F.collect_set("metric")).alias("num_metrics"),
    )
    .filter(F.col("amount_count") > 1)
)
