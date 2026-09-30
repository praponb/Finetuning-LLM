from pyspark.sql import functions as F

metrics_long = (
    df_orders
    .select("id", "status", "shipping_fee", F.col("amount").alias("AMOUNT"), "discount")
    .unpivot(["id", "status"], ["shipping_fee", "AMOUNT", "discount"], "kpi", "val")
    .filter(F.col("val").isNotNull())
)

result = (
    metrics_long
    .groupBy(F.when(F.col("val") >= 89, "LARGE").otherwise("SMALL").alias("val_tier"))
    .agg(
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("kpi") == "shipping_fee", F.col("val")).otherwise(0)).alias("shipping_fee_val"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.avg("val").alias("avg_val_2"),
    )
    .orderBy(F.col("shipping_fee_val").asc_nulls_last())
)
