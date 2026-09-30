from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy("cost_share", "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("cost_dense_rank").desc(), "id")
w_hist = Window.partitionBy("order_id")
w7 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_last())

joined = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.status").alias("order_status"))
)

filtered = (
    joined
    .filter(F.month("delivered_date").isin([7, 8, 9]))
    .select("*", F.date_trunc("month", "shipped_date").alias("shipped_date_period"))
)

enriched = (
    filtered
    .filter(F.year("delivered_date") == 2024)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "shipped_date_period",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
        F.sum("cost").over(w3).alias("cumulative_cost"),
    )
)

scored = (
    enriched
    .withColumn("prev_cumulative_cost", F.lag("cumulative_cost", 2, 0).over(w_ordered))
    .withColumn("cost_dense_rank_bucket", F.ntile(3).over(w5))
    .withColumn("order_id_stddev_samp_cost", F.stddev_samp("cost").over(w_hist))
    .withColumn("cost_rank", F.rank().over(w7))
)

enriched_shipments = (
    scored
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.delivered_date",
        "s.status",
        "s.cost_share",
        "s.cost_dense_rank",
        "s.cumulative_cost",
        "s.cost_dense_rank_bucket",
        "s.cost_rank",
        F.col("b.order_date"),
    )
)

joined_shipments = (
    enriched_shipments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.cost_share",
        "s.cumulative_cost",
        "s.cost_dense_rank_bucket",
        "s.cost_rank",
        F.col("d.amount"),
        F.col("d.discount"),
    )
)

result = joined_shipments.select("id", "cumulative_cost", "amount")
