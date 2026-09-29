from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(
        (F.max(F.col("o.discount")) - F.min(F.col("o.discount"))).alias("discount_range"),
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
    )
    .filter(F.col("pct_partner") > 20)
    .orderBy(F.col("pct_partner").desc(), F.col("opened_month").desc())
)
