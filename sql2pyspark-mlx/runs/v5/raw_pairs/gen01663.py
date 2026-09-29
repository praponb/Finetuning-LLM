from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").asc_nulls_first())
w_part = Window.orderBy("shipped_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("shipping_fee", "id")
w4 = Window.orderBy("cost", "id")
w5 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id", "status").orderBy(F.col("cost_bucket").desc(), "id")

deduped = (
    df_shipments
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.discount") <= 0.28)
    .select("a.*", F.col("b.amount"), F.col("b.shipping_fee"), F.col("b.channel"))
)

scored = (
    enriched
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        "amount",
        F.avg("cost").over(w_part).alias("cost_avg_2"),
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(5).over(w4).alias("cost_bucket"),
        F.first("channel", ignorenulls=True).over(w5).alias("first_known_channel"),
        F.sum("shipping_fee").over(w_ordered).alias("cumulative_shipping_fee"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "status", "amount", "cost_avg_2", "cost_bucket")
)

result = (
    latest
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.status").alias("order_status"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.amount")), 2).alias("total_amount"),
        F.min(F.col("s.cost_bucket")).alias("min_cost_bucket"),
    )
)
