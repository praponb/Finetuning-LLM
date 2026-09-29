from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("brand", "product_id").orderBy("unit_price")

cleaned = (
    df_order_items
    .filter(F.col("unit_price") > 290)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "product_id",
        "unit_price",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lag("line_total").over(w).alias("prev_line_total"),
        F.row_number().over(w).alias("seq_no"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.brand"))
)

deduped_order_items = (
    enriched
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    deduped_order_items
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.unit_price",
        "a.prev_quantity",
        "a.prev_line_total",
        "a.row_num",
        "a.latest_rank",
        F.col("b.stock_qty"),
    )
)

joined_order_items = (
    joined
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.supplier_id").isNotNull())
    .select(
        "a.id",
        "a.product_id",
        "a.unit_price",
        "a.prev_quantity",
        "a.prev_line_total",
        "a.row_num",
        "a.latest_rank",
        F.col("b.launch_date"),
        F.col("b.cost"),
    )
)

result = (
    joined_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .cube(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.cost")).alias("min_cost"),
        F.grouping_id().alias("level_id"),
    )
)
