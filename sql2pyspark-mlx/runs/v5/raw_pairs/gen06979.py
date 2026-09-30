from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([5, 13, 36])), "left_anti")
)

result = (
    kept
    .cube(F.date_trunc("month", "order_date").alias("activity_month"))
    .agg(
        F.count_if(F.col("channel") == "web").alias("web_events"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("shipping_fee_per_partner"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .filter(F.col("shipping_fee_per_partner") >= 0)
    .orderBy(F.col("shipping_fee_per_partner").desc_nulls_last())
)
