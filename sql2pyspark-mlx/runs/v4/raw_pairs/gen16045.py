from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.min("paid_at").alias("first_paid_at"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
    )
    .orderBy(F.col("amount_stddev").asc_nulls_first())
)
