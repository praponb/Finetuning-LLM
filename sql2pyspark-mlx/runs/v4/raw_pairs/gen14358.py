from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("cost", "id")
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.to_date(F.lit("2024-03-16"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.date_sub("delivered_date", 82).alias("delivered_date_minus_82d"),
        F.date_format("shipped_date", "MMM yyyy").alias("shipped_date_label"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

metrics = (
    cleaned
    .withColumn("cost_avg_6_prior", F.avg("cost").over(w_recent))
    .withColumn("cost_avg_6", F.avg("cost").over(w2))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w3))
    .withColumn("cost_bucket", F.ntile(10).over(w4))
)

with_windows = (
    metrics
    .withColumn("order_id_avg_cost_avg_6", F.avg("cost_avg_6").over(w5))
    .withColumn("cumulative_cost_avg_6_prior", F.sum("cost_avg_6_prior").over(w6))
    .withColumn("running_cost_avg_6", F.sum("cost_avg_6").over(w7))
)

result = (
    with_windows
    .select(
        "id",
        "cost",
        "status",
        "shipped_date_label",
        "resolved_carrier",
        "cost_avg_6",
        "last_known_carrier",
        "order_id_avg_cost_avg_6",
        "cumulative_cost_avg_6_prior",
    )
    .orderBy(F.col("cumulative_cost_avg_6_prior").desc(), F.col("status").desc())
)
