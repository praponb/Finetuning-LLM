from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("adjusted_amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-09-09 03:00:00").cast("timestamp"))
    .select("*", F.round((F.col("amount") * 0.9), 2).alias("adjusted_amount"))
)

enriched = (
    valid
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") <= 1300)
    .select("e.*", F.col("o.status").alias("order_status"), F.col("o.channel"))
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "amount", "paid_at", "status", "adjusted_amount")
)

with_windows = (
    ranked
    .select(
        "*",
        F.avg("amount").over(w2).alias("amount_avg_2"),
        F.count("adjusted_amount").over(w3).alias("adjusted_amount_count_90d"),
        F.max("adjusted_amount").over(w4).alias("status_max_adjusted_amount"),
        F.max("amount").over(w5).alias("amount_max_30d"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "adjusted_amount",
        "amount_avg_2",
        "adjusted_amount_count_90d",
        "amount_max_30d",
        F.when(F.col("status").rlike("^(REFUNDED|SUCCESS)$"), "OTHER_ZONE").when(F.col("status").rlike("^(FAILED)$"), "GROUP_B").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy("adjusted_amount")
)
