from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w2 = Window.partitionBy("city").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("last_restock_month_end").desc(), "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("units").alias("sum_store_sale_units"))

valid = (
    df_inventory
    .filter(F.col("quantity") >= 77)
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

joined = (
    valid
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.city"))
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    ranked
    .withColumn("quantity_bucket", F.ntile(10).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

enriched = (
    with_windows
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.last_restock",
        "s.quantity_bucket",
        "s.seq_no",
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("p.brand"),
    )
)

result = (
    enriched
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_bucket",
        "stock_qty",
        "price",
        "brand",
        F.col("sum_store_sale_units"),
    )
    .orderBy("id")
)
