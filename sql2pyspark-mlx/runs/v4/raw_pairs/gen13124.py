from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id").orderBy(F.col("line_total").asc_nulls_last())
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

valid = (
    df_order_items
    .filter(F.col("quantity") == 4)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

cleaned = (
    valid
    .filter(F.col("line_total").isNotNull())
    .select(
        "*",
        F.round((F.col("line_total") * 1.15), 2).alias("adjusted_line_total"),
        F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"),
    )
)

remaining_keys = (
    cleaned
    .select("order_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price") > 390)
        .select("order_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("product_id_max_adjusted_line_total", F.max("adjusted_line_total").over(w))
    .withColumn("line_total_cume_dist", F.cume_dist().over(w2))
)

report = (
    enriched
    .select(
        "product_id",
        "quantity",
        "adjusted_line_total",
        "adjusted_quantity",
        "product_id_max_adjusted_line_total",
    )
    .orderBy("product_id_max_adjusted_line_total")
)

result = (
    report
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "quantity",
        "adjusted_line_total",
        "adjusted_quantity",
        "product_id_max_adjusted_line_total",
        F.col("sum_order_item_quantity"),
    )
)
