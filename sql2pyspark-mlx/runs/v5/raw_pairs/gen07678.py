from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("line_total").desc(), "id")
w_recent = Window.partitionBy("order_id").orderBy("latest_rank", "id")
w3 = Window.orderBy(F.col("latest_rank").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("next_quantity").desc(), "id")
w5 = Window.orderBy(F.col("unit_price").desc(), "id")
w6 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc_nulls_first())
w7 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")
w9 = Window.partitionBy("order_id", "product_id").orderBy("prev_unit_price", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

filtered = (
    df_order_items
    .filter(F.col("quantity").between(4, 7))
    .withColumn("line_total", (F.col("unit_price") + F.col("quantity")))
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") < 190))
    .select("*", F.round("line_total", 1).alias("line_total_rounded"))
)

enriched = (
    cleaned
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "latest_rank",
        F.lag("unit_price").over(w_recent).alias("prev_unit_price"),
        F.lead("quantity").over(w_recent).alias("next_quantity"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

scored = (
    enriched
    .withColumn("row_seq_2", F.row_number().over(w3))
    .withColumn("next_quantity_bucket", F.ntile(5).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

deduped = (
    scored
    .withColumn("latest_rank_2", F.rank().over(w6))
    .filter(F.col("latest_rank_2") == 1)
)

enriched_order_items = (
    deduped
    .select(
        "id",
        "order_id",
        "prev_unit_price",
        "next_quantity",
        "event_seq",
        "latest_rank_2",
        F.sum("row_seq").over(w7).alias("running_row_seq"),
        F.ntile(3).over(w8).alias("unit_price_bucket"),
        F.row_number().over(w9).alias("row_seq_3"),
    )
)

result = (
    enriched_order_items
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "event_seq",
        "unit_price_bucket",
        F.col("sum_shipment_cost"),
    )
)
