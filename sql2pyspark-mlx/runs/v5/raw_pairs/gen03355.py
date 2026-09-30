from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.opened_date", F.col("c.amount").alias("order_amount"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("order_amount") >= 1640, "PREMIUM").otherwise("STANDARD").alias("order_amount_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("order_amount", 0.9).alias("order_amount_p90"),
        F.percentile_approx("order_amount", 0.95).alias("order_amount_p95"),
    )
)
