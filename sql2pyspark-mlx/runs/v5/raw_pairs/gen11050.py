from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").asc_nulls_last(), "id")
w2 = Window.partitionBy("method", "order_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("rank_in_group", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("running_count_amount").desc_nulls_last(), "id")

deduped = (
    df_payments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .filter(F.col("amount") >= 880)
    .select(
        "id",
        "order_id",
        "amount",
        "rank_in_group",
        F.avg("amount").over(w2).alias("running_avg_amount"),
        F.sum("amount").over(w2).alias("running_sum_amount"),
        F.count("amount").over(w2).alias("running_count_amount"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "inner")
    .select("a.*", F.col("p.order_date"), F.col("p.amount").alias("order_amount"))
)

latest = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.amount")).alias("max_amount"),
    )
    .orderBy(F.col("status").asc_nulls_last())
)
