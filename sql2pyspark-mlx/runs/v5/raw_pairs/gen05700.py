from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")
w4 = Window.partitionBy("category").orderBy(F.col("launch_date").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

latest = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "product_name", "category", "price", "stock_qty", "launch_date")
)

with_latest = (
    latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

filtered = (
    with_latest
    .filter(F.col("stock_qty").isNotNull())
    .withColumn("product_name_lower", F.lower("product_name"))
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "launch_date",
        "inventory_quantity",
    )
)

ranked_products = (
    ranked
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_latest_products = (
    ranked_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.stock_qty",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.sale_date"),
    )
)

result = (
    with_latest_products
    .groupBy(
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
        "category",
        F.when(F.col("store_sale_units") >= 35, "PREMIUM").otherwise("STANDARD").alias("store_sale_units_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("total_stock_qty") >= 0)
    .orderBy(F.col("week_start").desc())
)
