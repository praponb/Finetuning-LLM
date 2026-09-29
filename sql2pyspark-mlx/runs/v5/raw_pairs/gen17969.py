from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.amount").between(990, 1320)), "left_anti")
)

result = (
    kept
    .groupBy(
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        F.when(F.col("shipping_fee") >= 32, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .orderBy(F.col("year_start").desc())
)
