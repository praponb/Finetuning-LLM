from pyspark.sql import functions as F

unpivoted = (
    df_orders
    .select("id", "customer_id", "shipping_fee", "discount", "amount")
    .unpivot(
        ["id", "customer_id"],
        ["shipping_fee", "discount", "amount"],
        "kpi",
        "val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("val") >= 65, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
    )
    .agg(
        F.sum(F.when(F.col("kpi") == "discount", F.col("val")).otherwise(0)).alias("discount_val"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("kpi") == "amount", 1)), F.lit(0)), 2).alias("val_per_amount"),
        F.round(F.avg("val"), 2).alias("avg_val"),
    )
    .orderBy(F.col("val_per_amount").desc_nulls_first())
)
