from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
    .orderBy(F.col("amount_range").desc())
)
