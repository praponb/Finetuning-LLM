from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("carrier", "status").orderBy("cost", "id")
w2 = Window.partitionBy("status").orderBy(F.col("prev_cost").desc_nulls_last(), "id")

scored = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        F.lead("cost").over(w_recent).alias("next_cost"),
        F.lag("cost").over(w_recent).alias("prev_cost"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.status"))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    latest
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.*",
        F.col("p.status").alias("order_status"),
        F.col("p.channel"),
        F.col("p.discount"),
    )
)

result = (
    with_dims
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .rollup(
        F.col("o.channel").alias("order_channel"),
        F.col("o.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.cost")).alias("max_cost"),
        F.sum(F.col("s.next_cost")).alias("total_next_cost"),
    )
    .orderBy("order_status")
)
