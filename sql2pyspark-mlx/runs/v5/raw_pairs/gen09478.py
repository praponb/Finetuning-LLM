from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("supplier_id")
w3 = Window.orderBy(F.col("price").desc(), "id")
w4 = Window.orderBy(F.col("price_avg_2").desc())
w_part = Window.partitionBy("supplier_id").orderBy(F.col("price").asc_nulls_first())
w_running = Window.orderBy(F.col("stock_qty").asc_nulls_first())

base = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-02-27")))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)

enriched = (
    base
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "supplier_id",
        "brand_title",
        F.avg("price").over(w).alias("price_avg_2"),
        F.min("cost").over(w2).alias("supplier_id_min_cost"),
        F.ntile(3).over(w3).alias("price_bucket"),
    )
)

kept_keys = (
    enriched
    .select("id", "supplier_id")
    .intersectAll(
        df_products
        .filter(F.lower(F.trim("product_name")) == "running shoes")
        .select("id", "supplier_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.units") > 21), "left_anti")
)

enriched_products = (
    not_flagged
    .filter(F.col("price_avg_2") == 779)
    .withColumn("price_avg_2_cume_dist", F.cume_dist().over(w4))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
    .withColumn("price_cume_dist", F.cume_dist().over(w_part))
    .withColumn("supplier_id_max_stock_qty", F.max("stock_qty").over(w2))
    .withColumn("stock_qty_cume_dist", F.cume_dist().over(w_running))
)

result = (
    enriched_products
    .groupBy("supplier_id")
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.percentile_approx("price_avg_2", [0.25, 0.5, 0.75]).alias("price_avg_2_quartiles"),
        F.percentile_approx("cost_share", 0.5).alias("cost_share_p50"),
        F.round(F.avg("stock_qty_cume_dist"), 2).alias("avg_stock_qty_cume_dist"),
    )
)
