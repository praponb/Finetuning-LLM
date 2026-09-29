from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc_nulls_first())

filtered = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-07-20"))) & F.col("product_name").isNotNull())
    .withColumn("category_upper", F.upper("category"))
)

with_dims = (
    filtered
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "inner")
    .filter(F.col("d.rating") > 3.9)
    .select("a.*", F.col("d.city"), F.col("d.country"))
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.launch_date")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        "country",
        F.corr("price", "stock_qty").over(w).alias("corr_price_stock_qty"),
        F.percent_rank().over(w2).alias("price_percent_rank"),
    )
)

not_flagged_products = (
    enriched
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.unit_price").between(280, 300)), "left_anti")
)

enriched_products = (
    not_flagged_products
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.supplier_name"))
)

joined = (
    enriched_products
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.city"))
)

result = (
    joined
    .groupBy("country")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.round(F.avg("price_percent_rank"), 2).alias("avg_price_percent_rank"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
    )
    .orderBy(F.col("avg_price_percent_rank").asc_nulls_last())
)
