from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("cost", "id")
w3 = Window.partitionBy("status").orderBy(F.col("cost_bucket_2").desc())
w_hist = Window.partitionBy("carrier").orderBy("cost_bucket_2", "id")
w5 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w6 = Window.partitionBy("order_id")
w7 = Window.partitionBy("status", "order_id").orderBy(F.col("prev_cost_bucket").desc_nulls_first(), "id")

with_windows = (
    df_shipments
    .select(
        "*",
        F.ntile(4).over(w).alias("cost_bucket"),
        F.ntile(10).over(w2).alias("cost_bucket_2"),
    )
)

scored = (
    with_windows
    .withColumn("cost_bucket_2_cume_dist", F.cume_dist().over(w3))
    .withColumn("prev_cost_bucket", F.lag("cost_bucket", 2).over(w_hist))
)

joined = (
    scored
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.amount"))
)

enriched = (
    joined
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("cost_bucket_2_share", F.round((F.col("cost_bucket_2") / F.sum("cost_bucket_2").over(w6)), 4))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "status",
        F.col("amount").alias("AMOUNT"),
        "cost_bucket_2_cume_dist",
        F.col("cost").alias("COST"),
        F.col("cost_bucket_2_share").alias("COST_BUCKET_2_SHARE_VAL"),
    )
    .unpivot(
        ["id", "status"],
        ["AMOUNT", "cost_bucket_2_cume_dist", "COST", "COST_BUCKET_2_SHARE_VAL"],
        "kpi",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
