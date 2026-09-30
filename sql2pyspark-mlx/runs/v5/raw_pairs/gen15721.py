from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last())
w5 = Window.partitionBy("status").orderBy("cost", "id")
w6 = Window.orderBy(F.col("cost_bucket").desc_nulls_first())
w7 = Window.orderBy("cost")
w8 = Window.partitionBy("order_id").orderBy(F.col("next_cost").asc_nulls_last())
w9 = Window.partitionBy("order_id").orderBy(F.col("cost_bucket_2").asc_nulls_first())
w10 = Window.partitionBy("carrier")
w11 = Window.orderBy(F.col("cost_rank").asc_nulls_first())
w12 = Window.partitionBy("status").orderBy(F.col("cost_bucket_share").desc())

with_windows = (
    df_shipments
    .filter(F.col("cost") < 45)
    .withColumn("cost_bucket", F.ntile(10).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("cost_bucket_2", F.ntile(5).over(w3))
    .withColumn("cost_rank", F.rank().over(w4))
    .withColumn("next_cost", F.lead("cost", 1).over(w5))
)

kept_keys = (
    with_windows
    .select("carrier", "id")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("carrier", "id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_shipments = (
    reconciled
    .select(
        "*",
        F.rank().over(w6).alias("cost_bucket_rank"),
        F.percent_rank().over(w7).alias("cost_percent_rank"),
        F.rank().over(w8).alias("next_cost_rank"),
        F.dense_rank().over(w9).alias("cost_bucket_2_dense_rank"),
        F.round((F.col("cost_bucket") / F.sum("cost_bucket").over(w10)), 4).alias("cost_bucket_share"),
    )
)

with_windows_shipments_2 = (
    with_windows_shipments
    .withColumn("cost_rank_rank", F.rank().over(w11))
    .withColumn("cost_bucket_share_percent_rank", F.percent_rank().over(w12))
    .withColumn("next_cost_rank_share", F.round((F.col("next_cost_rank") / F.sum("next_cost_rank").over(w10)), 4))
)

result = (
    with_windows_shipments_2
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_bucket",
        "row_seq",
        "cost_bucket_2",
    )
    .orderBy(F.col("delivered_date").asc_nulls_first())
)
