from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("rating_tier").orderBy(F.col("new_york_price").desc())

eligible = (
    df_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.launch_date")), "left_semi")
)

with_dims = (
    eligible
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.country"), F.col("p2.rating"), F.col("p2.city"))
)

with_dims_products = (
    with_dims
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.rating").alias("supplier_rating"),
        F.col("p.supplier_name"),
        F.col("p.country").alias("supplier_country"),
    )
)

daily = (
    with_dims_products
    .groupBy(
        F.when(F.col("rating") >= 3.8, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        F.when(F.col("stock_qty") >= 301, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("price")).otherwise(0)).alias("new_york_price"),
        F.sum(F.when(F.col("supplier_country") == "TH", F.col("stock_qty")).otherwise(0)).alias("th_stock_qty"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sum(F.when(F.col("country") == "JP", F.col("supplier_rating"))).alias("jp_total_supplier_rating"),
    )
    .withColumn("new_york_price_rank", F.rank().over(w))
)

result = (
    daily
    .select(
        "rating_tier",
        "stock_qty_tier",
        "new_york_price",
        "th_stock_qty",
        "num_countries",
        "jp_total_supplier_rating",
        "new_york_price_rank",
    )
    .orderBy("rating_tier")
)
