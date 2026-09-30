from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").asc_nulls_first(), "id")

deduped = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "amount", "paid_at", "status")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("amount") >= 1630, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg(F.when(F.col("status") == "REFUNDED", F.col("amount"))).alias("refunded_avg_amount"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .orderBy(F.col("amount_stddev").asc_nulls_last())
)
