from pyspark.sql import functions as F

kept_keys = (
    df_orders
    .select("channel")
    .subtract(
        df_orders
        .filter(F.col("status") != "SHIPPED")
        .select("channel")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        F.when(F.col("shipping_fee") >= 42, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
    )
)
