from pyspark.sql import functions as F

not_flagged = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") > 160), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        "store_id",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("amount_per_mobile"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
    )
    .filter(F.col("amount_per_mobile") >= 0)
)
