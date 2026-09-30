from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city").isin(["Singapore", "Chiang Mai", "New York"]))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("s.opened_date"),
        F.coalesce(F.col("p.amount"), F.lit(0)).alias("amount_filled"),
    )
    .orderBy(F.col("id").desc())
)
