from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy("delivered_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status", "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("discount", "id")
w_recent = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_status").orderBy("cost_bucket", "id")
w7 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.discount"), F.col("d.channel"))
)

metrics = (
    with_dims
    .withColumn("discount_avg_5_prior", F.avg("discount").over(w))
    .withColumn("cost_bucket", F.ntile(10).over(w2))
    .withColumn("running_discount", F.sum("discount").over(w3))
    .withColumn("discount_bucket", F.ntile(4).over(w4))
)

enriched = (
    metrics
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.order_date"), F.col("b.status").alias("order_status"))
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.first("discount_avg_5_prior", ignorenulls=True).over(w_recent).alias("first_known_discount_avg_5_prior"),
        F.lead("discount", 2).over(w6).alias("next_discount"),
        F.first("cost_bucket", ignorenulls=True).over(w7).alias("first_known_cost_bucket"),
    )
)

result = (
    enriched_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "discount",
        "discount_avg_5_prior",
        "cost_bucket",
        "running_discount",
    )
    .orderBy(F.col("cost_bucket").desc_nulls_last())
)
