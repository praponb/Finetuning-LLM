from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last(), "id")

deduped = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupBy(F.date_trunc("year", "paid_at").alias("year_start"), "order_id")
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
    )
    .filter(F.col("amount_p95") >= 0)
    .orderBy(F.col("order_id").desc_nulls_first())
)
