from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.partitionBy("method").orderBy(F.col("running_amount").desc_nulls_first(), "id")

metrics = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.sum("amount").over(w).alias("running_amount"),
        F.sum("amount").over(w2).alias("running_amount_2"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    latest
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "running_amount",
        "running_amount_2",
        "rank_in_group",
        F.trim("method").alias("clean_method"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group_2", F.row_number().over(w4))
    .filter(F.col("rank_in_group_2") == 1)
)

result = (
    ranked
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "running_amount",
        "rank_in_group",
        "clean_method",
        "rank_in_group_2",
    )
)
