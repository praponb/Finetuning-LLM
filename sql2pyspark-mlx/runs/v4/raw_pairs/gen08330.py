from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("shipping_fee", 0.9).alias("shipping_fee_p90"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_per_store"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
    )
    .orderBy(F.col("shipping_fee_p90").desc_nulls_first())
)
