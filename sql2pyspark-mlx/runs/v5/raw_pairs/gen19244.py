from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")

deduped = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.max("amount").alias("max_amount"),
        F.avg(F.when(F.col("status") == "FAILED", F.col("amount"))).alias("failed_avg_amount"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.min("paid_at").alias("first_paid_at"),
    )
)
