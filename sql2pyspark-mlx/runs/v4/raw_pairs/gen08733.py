from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount_stddev").desc())

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.max("paid_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
    )
    .withColumn("amount_stddev_rank", F.rank().over(w))
    .orderBy(F.col("method").desc_nulls_last())
)
