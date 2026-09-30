from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("amount", "id")
w5 = Window.partitionBy("order_id").orderBy("next_amount", "id")
w6 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("method")
w8 = Window.partitionBy("status").orderBy("running_amount", "id")
w9 = Window.orderBy("running_amount_count_30d", "id")
w10 = Window.orderBy("paid_at_date", "id")

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-04-28 04:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.upper("method").alias("method_upper"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

metrics = (
    valid
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("order_id_stddev_samp_amount", F.stddev_samp("amount").over(w2))
    .withColumn("next_amount", F.lead("amount", 1).over(w3))
)

enriched = (
    metrics
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.lead("next_amount", 2, 0).over(w5).alias("next_next_amount"),
    )
)

enriched_payments = (
    enriched
    .select(
        "*",
        F.count("running_amount").over(w6).alias("running_amount_count_30d"),
        F.round((F.col("next_next_amount") / F.sum("next_next_amount").over(w7)), 4).alias("next_next_amount_share"),
    )
)

result = (
    enriched_payments
    .withColumn("running_amount_bucket", F.ntile(5).over(w8))
    .withColumn("next_running_amount", F.lead("running_amount", 2).over(w9))
    .withColumn("event_seq_2", F.row_number().over(w10))
)
