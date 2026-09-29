from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy("delivered_date_plus_months", "id")
w2 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel", "status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel", "carrier").orderBy("cost_avg_5", "id")

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.add_months("delivered_date", 3).alias("delivered_date_plus_months"))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.channel"), F.col("d.discount"))
)

metrics = (
    enriched
    .withColumn("prev_discount", F.lag("discount", 1).over(w))
    .withColumn("cost_avg_5", F.avg("cost").over(w2))
    .withColumn("running_discount", F.sum("discount").over(w3))
    .withColumn("running_discount_2", F.sum("discount").over(w4))
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "status",
        F.col("cost").alias("COST"),
        "discount",
        F.col("prev_discount").alias("PREV_DISCOUNT_VAL"),
    )
    .unpivot(
        ["id", "status"],
        ["COST", "discount", "PREV_DISCOUNT_VAL"],
        "metric",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
