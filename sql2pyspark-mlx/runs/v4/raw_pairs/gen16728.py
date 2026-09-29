from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

with_last = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

result = (
    with_last
    .rollup(
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.corr("discount", "shipping_fee").alias("corr_discount_shipping_fee"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("shipping_fee_per_web"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("shipping_fee")).otherwise(0)).alias("completed_shipping_fee"),
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.count_if(F.col("status") == "COMPLETED").alias("completed_events"),
    )
    .orderBy(F.col("cnt").desc())
)
