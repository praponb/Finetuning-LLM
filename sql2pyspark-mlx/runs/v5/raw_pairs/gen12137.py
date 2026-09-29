from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-05-03").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.regexp_replace("product_name", " ", "_").alias("product_name_slug"),
        F.upper("product_name").alias("product_name_upper"),
    )
)

enriched = (
    filtered
    .filter(F.col("stock_qty") > 207)
    .select(
        "*",
        F.sum("cost").over(w).alias("last4_sum_cost"),
        F.count("cost").over(w).alias("last4_count_cost"),
        F.max("cost").over(w).alias("last4_max_cost"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.stock_qty",
        "s.supplier_id",
        "s.last4_sum_cost",
        "s.last4_count_cost",
        F.col("c.quantity"),
    )
)

cleaned = (
    with_latest
    .filter(F.col("supplier_id") == 8)
    .select(
        "id",
        "stock_qty",
        "last4_sum_cost",
        "last4_count_cost",
        "quantity",
        F.round((F.col("last4_sum_cost") * 1.1), 2).alias("last4_sum_cost_scaled"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") != 6), "left_anti")
)

result = (
    eligible
    .select("id", "stock_qty", "last4_sum_cost", "last4_count_cost")
    .orderBy(F.col("last4_sum_cost").asc_nulls_first())
)
