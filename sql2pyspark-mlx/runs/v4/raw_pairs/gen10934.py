from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w3 = Window.partitionBy("supplier_id").orderBy("price", "id")
w4 = Window.partitionBy("supplier_country", "city").orderBy("price_bucket_3", "id")
w_recent = Window.partitionBy("brand", "category").orderBy(F.col("price_bucket").desc(), "id")
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("supplier_country_2").orderBy(F.col("price_bucket_2").desc(), "id")
w_ordered = Window.orderBy("price_bucket_2", "id")
w9 = Window.partitionBy("supplier_country_2")

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-05-02").cast("date")) & F.col("brand").isNotNull() & F.array_contains("tags", "clearance"))
    .select(
        "*",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price").between(170, 230)), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.ntile(10).over(w).alias("price_bucket"),
        F.ntile(5).over(w2).alias("price_bucket_2"),
        F.ntile(4).over(w3).alias("price_bucket_3"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.country"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.city"),
        F.col("p.country").alias("supplier_country"),
        F.col("p.supplier_name"),
    )
)

metrics_products = (
    enriched
    .withColumn("price_bucket_3_bucket", F.ntile(3).over(w4))
    .withColumn("price_bucket_bucket", F.ntile(3).over(w_recent))
    .withColumn("country_sum_stock_qty", F.sum("stock_qty").over(w6))
)

with_dims_products = (
    metrics_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.city").alias("supplier_city"),
        F.col("d.country").alias("supplier_country_2"),
    )
)

result = (
    with_dims_products
    .select(
        "*",
        F.ntile(4).over(w7).alias("price_bucket_2_bucket"),
        F.lead("stock_qty", 1).over(w_ordered).alias("next_stock_qty"),
        F.lead("price_bucket_2", 2).over(w_ordered).alias("next_price_bucket_2"),
        F.round((F.col("price_bucket") / F.sum("price_bucket").over(w9)), 4).alias("price_bucket_share"),
    )
)
