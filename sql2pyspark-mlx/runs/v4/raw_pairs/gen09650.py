from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc_nulls_first())
w2 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("cost", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last())
w5 = Window.partitionBy("carrier").orderBy("cost")
w6 = Window.orderBy(F.col("cost").desc())

ranked = (
    df_shipments
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    latest
    .filter((F.col("delivered_date") > F.to_date(F.lit("2024-04-05"))) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

result = (
    base
    .withColumn("cost_bucket", F.ntile(4).over(w3))
    .withColumn("cost_percent_rank", F.percent_rank().over(w4))
    .withColumn("cost_dense_rank", F.dense_rank().over(w5))
    .withColumn("cost_dense_rank_2", F.dense_rank().over(w6))
)
