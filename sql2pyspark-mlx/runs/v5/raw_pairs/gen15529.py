from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.delivered_date") > "2025-09-07")
    .groupBy(
        F.when(F.year(F.col("o.order_date")) < 2024, "before_2024").when(F.year(F.col("o.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("s.status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
        F.round(((100 * F.sum(F.when(F.col("s.status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
    )
    .orderBy(F.col("order_cohort").desc())
)
