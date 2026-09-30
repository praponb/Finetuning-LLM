from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "supplier_id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.year("launch_date") == 2025))
    .select(
        "*",
        F.substring("category", 1, 4).alias("category_prefix"),
        F.size("tags").alias("tags_count"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.first("tags_count", ignorenulls=True).over(w).alias("first_known_tags_count"),
        F.sum("price").over(w2).alias("category_sum_price"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

valid = (
    with_last
    .filter(F.col("launch_date") > F.lit("2023-08-05").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "category_prefix",
        "first_known_tags_count",
        "category_sum_price",
        "unit_price",
        (F.col("first_known_tags_count") + F.col("tags_count")).alias("first_known_tags_count_plus_tags_count"),
        (F.col("cost") / F.nullif("stock_qty", F.lit(0))).alias("cost_per_stock_qty"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.launch_date")), "left_semi")
)

result = (
    not_flagged
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 420, "HIGH").otherwise("LOW"))
    .rollup("unit_price_tier")
    .agg(
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.grouping("unit_price_tier").alias("grouping_id"),
    )
    .filter(F.col("num_brands") < 1)
    .orderBy(F.col("distinct_brands").desc())
)
