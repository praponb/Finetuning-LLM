from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w4 = Window.partitionBy("product_id", "status").orderBy("line_total", "id")
w_recent = Window.partitionBy("order_id").orderBy("next_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_order_items
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.lead("unit_price", 2).over(w2).alias("next_unit_price"),
    )
)

joined = (
    with_windows
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.status").isin(["PENDING", "SHIPPED", "CANCELLED"]))
    .select("e.*", F.col("o.status"))
)

cleaned = (
    joined
    .filter(F.col("product_id").isNotNull() & (~F.col("status").eqNullSafe(F.lit("SHIPPED"))))
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
    .withColumn("adjusted_row_seq", F.round((F.col("row_seq") * 100), 2))
    .withColumn("resolved_status", F.coalesce("status", F.lit("OTHER")))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "order_id",
        "unit_price",
        "next_unit_price",
        "status",
        "resolved_status",
    )
)

valid = (
    ranked
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        "next_unit_price",
        "status",
        "resolved_status",
        F.floor("next_unit_price").alias("next_unit_price_whole"),
    )
)

base = (
    valid
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "order_id",
        "next_unit_price",
        "resolved_status",
        "next_unit_price_whole",
        F.when(F.col("next_unit_price_whole") >= 71, "High").when(F.col("next_unit_price_whole") >= 27, "Medium").otherwise("Low").alias("next_unit_price_whole_band"),
    )
)

result = (
    base
    .select(
        "id",
        "order_id",
        "next_unit_price_whole",
        F.max("next_unit_price").over(w_recent).alias("running_max_next_unit_price"),
        F.sum("next_unit_price").over(w_recent).alias("running_sum_next_unit_price"),
        F.count("next_unit_price").over(w_recent).alias("running_count_next_unit_price"),
        F.avg("next_unit_price").over(w_recent).alias("running_avg_next_unit_price"),
        F.last("next_unit_price", ignorenulls=True).over(w_recent).alias("last_known_next_unit_price"),
    )
)
