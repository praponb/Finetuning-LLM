from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.orderBy("discount", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("order_id").orderBy("amount", "id")

latest = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank_2", F.row_number().over(w2))
    .filter(F.col("latest_rank_2") == 1)
    .select("id", "order_id")
)

with_dims = (
    ranked
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.id", "e.order_id", F.col("p.discount"), F.col("p.amount"))
)

metrics = (
    with_dims
    .select(
        "id",
        "order_id",
        "discount",
        "amount",
        F.rank().over(w3).alias("amount_rank"),
        F.avg("amount").over(w4).alias("amount_avg_3_prior"),
        F.lead("amount", 1).over(w5).alias("next_amount"),
    )
)

result = (
    metrics
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .cube(F.col("d.channel").alias("channel"), F.col("d.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.amount")).alias("min_amount"),
        F.round(F.sum(F.col("s.amount_avg_3_prior")), 2).alias("total_amount_avg_3_prior"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("min_amount").desc())
)
