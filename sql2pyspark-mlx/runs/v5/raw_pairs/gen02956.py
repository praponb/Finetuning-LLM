from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc_nulls_last(), "id")
w2 = Window.partitionBy("supplier_id", "category").orderBy(F.col("price").asc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

ranked = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isNotNull()), "left_anti")
)

base = (
    not_flagged
    .filter((F.col("launch_date") >= F.lit("2023-05-21").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "row_num",
        (F.col("price") + F.col("stock_qty")).alias("price_plus_stock_qty"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

ranked_products = (
    base
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    ranked_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.stock_qty",
        "s.launch_date",
        "s.supplier_id",
        "s.row_num",
        "s.brand_filled",
        "s.latest_rank",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

joined = (
    with_last
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.stock_qty",
        "s.supplier_id",
        "s.row_num",
        "s.order_item_quantity",
        F.col("p2.supplier_name"),
        F.col("p2.city"),
        F.col("p2.country"),
    )
)

result = (
    joined
    .select("id", "order_item_quantity", "city", "country")
    .orderBy(F.col("country").asc_nulls_last())
)
