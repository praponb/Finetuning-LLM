from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w4 = Window.partitionBy("status", "order_id").orderBy(F.col("seq_no_2").desc_nulls_last(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.max("quantity").alias("max_order_item_quantity"))

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("delivered_date").between("2024-09-19", "2025-01-11"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.round((F.col("cost") * 0.9), 2).alias("cost_adjusted"),
    )
)

with_windows = (
    valid
    .withColumn("cost_bucket", F.ntile(5).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost_adjusted",
        "seq_no",
        "seq_no_2",
        F.col("max_order_item_quantity"),
    )
    .orderBy(F.col("cost_adjusted").desc())
)
