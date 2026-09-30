from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("status_length", "id")
w2 = Window.partitionBy("order_status").orderBy(F.col("status_length").desc(), "id")
w_grp = Window.partitionBy("carrier")
w4 = Window.partitionBy("order_status").orderBy("status_length_bucket", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("order_status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("carrier").orderBy(F.col("status_length_bucket").desc_nulls_first())
w7 = Window.partitionBy("status").orderBy(F.col("status_length_bucket_dense_rank").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2024-01-26").cast("date"))
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.status").alias("order_status"))
)

enriched_shipments = (
    enriched
    .withColumn("next_cost", F.lead("cost", 1).over(w))
    .withColumn("status_length_bucket", F.ntile(4).over(w2))
)

with_windows = (
    enriched_shipments
    .withColumn("carrier_count_status_length", F.count("status_length").over(w_grp))
    .withColumn("cost_avg_5_prior", F.avg("cost").over(w4))
    .withColumn("cumulative_status_length", F.sum("status_length").over(w5))
    .withColumn("status_length_bucket_dense_rank", F.dense_rank().over(w6))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "shipped_date",
        "cost",
        "status_length",
        "next_cost",
        "status_length_bucket",
        "cost_avg_5_prior",
        "cumulative_status_length",
        "status_length_bucket_dense_rank",
    )
    .orderBy("cumulative_status_length", "cost")
)
