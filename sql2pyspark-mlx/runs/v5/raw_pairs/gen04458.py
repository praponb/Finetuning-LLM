from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("corr_discount_amount").desc_nulls_first())
)
