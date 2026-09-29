from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("amount_tier").orderBy(F.col("duration_min").desc())

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1630, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
    )
    .agg(
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.stddev_samp("amount").alias("amount_stddev"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
    )
    .withColumn("duration_min_percent_rank", F.percent_rank().over(w))
)
