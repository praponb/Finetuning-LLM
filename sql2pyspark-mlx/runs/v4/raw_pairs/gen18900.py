from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w_ordered = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w_part = Window.partitionBy("status").orderBy("cost", "id")
w_part_2 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("status", "carrier").orderBy("cost_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("shipped_date", "id")
w_recent = Window.partitionBy("order_id").orderBy("last_known_cost_rounded", "id")
w8 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_id").orderBy("cost_bucket", "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.min("quantity").alias("min_order_item_quantity"))

base = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .withColumn("cost_rounded", F.round("cost", 2))
    .withColumn("clean_carrier", F.trim("carrier"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("NONE")))
)

enriched = (
    base
    .withColumn("carrier_sum_cost_rounded", F.sum("cost_rounded").over(w))
    .withColumn("cost_bucket", F.ntile(5).over(w_ordered))
    .withColumn("cost_bucket_2", F.ntile(10).over(w_part))
    .withColumn("row_seq", F.row_number().over(w_part_2))
    .withColumn("cost_bucket_3", F.ntile(3).over(w_part))
)

metrics = (
    enriched
    .select(
        "*",
        F.last("cost_rounded", ignorenulls=True).over(w5).alias("last_known_cost_rounded"),
        F.lead("cost_bucket_3", 2).over(w6).alias("next_cost_bucket_3"),
    )
)

enriched_shipments = (
    metrics
    .withColumn("next_cost_rounded", F.lead("cost_rounded", 1, 0).over(w_recent))
    .withColumn("last_known_next_cost_bucket_3", F.last("next_cost_bucket_3", ignorenulls=True).over(w8))
    .withColumn("next_cost_rounded_2", F.lead("cost_rounded", 1).over(w9))
)

result = (
    enriched_shipments
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "clean_carrier",
        "resolved_carrier",
        "cost_bucket",
        F.col("min_order_item_quantity"),
    )
)
