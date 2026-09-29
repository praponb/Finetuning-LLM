from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc())
w2 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-3, -1)

deduped = (
    df_shipments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    deduped
    .select(
        "*",
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.avg("cost").over(w3).alias("cost_avg_3_prior"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.shipped_date",
        "src.delivered_date",
        "src.cost",
        "src.cost_avg_3_prior",
        F.col("b.discount"),
        F.col("b.shipping_fee"),
    )
)

base = (
    with_dims
    .filter(F.col("shipped_date") > F.lit("2023-02-19").cast("date"))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "cost_avg_3_prior",
        "discount",
        "shipping_fee",
        F.least("cost", "cost_avg_3_prior").alias("least_cost_cost_avg_3_prior"),
    )
)

report = (
    base
    .select("id", "discount", "shipping_fee", "least_cost_cost_avg_3_prior")
    .orderBy("id", F.col("shipping_fee").desc())
)

result = report.select("discount", "shipping_fee", "least_cost_cost_avg_3_prior")
