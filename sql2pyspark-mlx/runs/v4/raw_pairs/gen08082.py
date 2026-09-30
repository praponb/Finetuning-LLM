from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") > 27) & ((F.col("channel") != "partner") | (F.col("amount") <= 1260)))
    .groupBy(
        F.when(F.col("discount") >= 0.39, "high").when(F.col("discount") >= 0.16, "mid").otherwise("low").alias("discount_bucket"),
        "status",
        "channel",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("discount")).otherwise(0)).alias("completed_discount"),
    )
    .filter(F.col("pct_partner") >= 47)
)
