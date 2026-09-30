from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc_nulls_first())
w2 = Window.partitionBy("order_id").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_order_items
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_dims = (
    latest
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.quantity",
        "e.unit_price",
        F.col("b.channel"),
        F.col("b.order_date"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.unit_price",
        "s.order_date",
        F.col("b.discount"),
        F.col("b.amount"),
    )
)

scored = (
    joined
    .select(
        "id",
        "order_id",
        "unit_price",
        "order_date",
        "amount",
        F.max("discount").over(w2).alias("last7_max_discount"),
        F.count("discount").over(w2).alias("last7_count_discount"),
        F.min("discount").over(w2).alias("last7_min_discount"),
        F.first("discount", ignorenulls=True).over(w3).alias("first_known_discount"),
    )
)

report = scored.select("id", "unit_price", "last7_max_discount")

result = (
    report
    .select("unit_price", "last7_max_discount")
    .orderBy(F.col("last7_max_discount").desc())
)
