from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("cost", "id")
w2 = Window.partitionBy("carrier", "status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("last_known_cost", "id")
w4 = Window.orderBy("shipped_date", "id")
w5 = Window.partitionBy("order_id").orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("cost_bucket", "id")
w7 = Window.partitionBy("status").orderBy(F.col("cost_bucket").desc(), "id")

latest = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    latest
    .withColumn("cost_bucket", F.ntile(3).over(w))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w2))
)

enriched_shipments = (
    enriched
    .withColumn("last_known_cost_bucket", F.ntile(5).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("cumulative_cost", F.sum("cost").over(w5))
    .withColumn("next_cost_bucket", F.lead("cost_bucket", 1).over(w6))
    .withColumn("cost_bucket_bucket", F.ntile(3).over(w7))
)

valid = (
    enriched_shipments
    .filter(F.col("cost") > 69)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "cost_bucket",
        "last_known_cost",
        "last_known_cost_bucket",
        "cumulative_cost",
        "next_cost_bucket",
        (F.col("cost") * F.col("cost_bucket")).alias("cost_times_cost_bucket"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost_bucket",
        "last_known_cost",
        "last_known_cost_bucket",
        "cumulative_cost",
    )
    .orderBy(F.col("id").desc())
)
