from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("cumulative_unit_price").asc_nulls_first(), "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.product_id",
        "e.unit_price",
        "e.line_total",
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
)

enriched_order_items = (
    enriched
    .select(
        "id",
        "order_id",
        "unit_price",
        "launch_date",
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.count("cost").over(w).alias("running_count_cost"),
        F.max("cost").over(w).alias("running_max_cost"),
        F.sum("unit_price").over(w2).alias("cumulative_unit_price"),
    )
)

joined = (
    enriched_order_items
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.amount") < 430)
    .select(
        "src.id",
        "src.order_id",
        "src.running_sum_cost",
        "src.running_count_cost",
        "src.cumulative_unit_price",
        F.col("p.amount"),
        F.col("p.order_date"),
    )
)

enriched_order_items_2 = (
    joined
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.*",
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("o.amount").alias("order_amount"),
    )
)

ranked = (
    enriched_order_items_2
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "running_count_cost",
        "cumulative_unit_price",
        "amount",
        "order_date",
        "shipping_fee",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
