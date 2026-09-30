from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").isNotNull() & F.col("order_id").isNotNull() & F.col("cost").between(36, 82))
    .groupBy(
        F.when(F.col("carrier").isin(["UPS", "Kerry"]), "tier_1").otherwise("other").alias("carrier_group"),
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
    .orderBy(F.col("pct_returned").desc())
)
