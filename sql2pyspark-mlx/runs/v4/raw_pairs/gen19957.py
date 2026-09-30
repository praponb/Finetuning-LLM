from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

ranked = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy("order_id")
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids_2"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.max("paid_at").alias("last_seen"),
    )
    .orderBy("span_minutes")
)
