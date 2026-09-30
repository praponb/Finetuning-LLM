from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("brand").orderBy("quantity_per_least_unit_price_quantity", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("least_unit_price_quantity", "id")

valid = (
    df_order_items
    .filter(F.col("order_id").isin([26, 29]))
    .select("*", F.least("unit_price", "quantity").alias("least_unit_price_quantity"))
)

filtered = (
    valid
    .filter(F.col("quantity") > 3)
    .select(
        "*",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
        (F.col("quantity") / F.nullif("least_unit_price_quantity", F.lit(0))).alias("quantity_per_least_unit_price_quantity"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.quantity",
        "src.least_unit_price_quantity",
        "src.line_total",
        "src.quantity_per_least_unit_price_quantity",
        F.col("d.price"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        "e.least_unit_price_quantity",
        "e.quantity_per_least_unit_price_quantity",
        F.col("d.brand"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.avg("least_unit_price_quantity").over(w_part).alias("least_unit_price_quantity_avg_5"),
        F.ntile(10).over(w2).alias("least_unit_price_quantity_bucket"),
    )
)

with_dims_order_items = (
    metrics
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.array_contains(F.col("b.tags"), "premium"))
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.least_unit_price_quantity",
        "s.brand",
        F.col("b.product_name"),
        F.col("b.launch_date"),
    )
)

result = (
    with_dims_order_items
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
        F.when(F.col("quantity") >= 9, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.avg(F.when(F.col("brand") == "Acme", F.col("least_unit_price_quantity"))).alias("acme_avg_least_unit_price_quantity"),
    )
    .orderBy(F.col("acme_avg_least_unit_price_quantity").desc_nulls_last())
)
