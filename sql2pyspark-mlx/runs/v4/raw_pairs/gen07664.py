from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.orderBy(F.col("stock_qty").asc_nulls_last())
w3 = Window.partitionBy("category", "supplier_id")
w4 = Window.partitionBy("supplier_id", "category")
w5 = Window.partitionBy("category").orderBy(F.col("category_avg_cost").asc_nulls_last())
w6 = Window.partitionBy("category").orderBy(F.col("supplier_id_stddev_samp_cost").desc())
w7 = Window.partitionBy("city").orderBy(F.col("supplier_id_max_supplier_id_stddev_samp_cost").desc_nulls_last())
w8 = Window.partitionBy("city")
w9 = Window.partitionBy("country")
w10 = Window.partitionBy("city").orderBy(F.col("corr_stock_qty_price").desc())
w11 = Window.orderBy(F.col("supplier_id_max_supplier_id_stddev_samp_cost_dense_rank").desc())
w12 = Window.partitionBy("brand")

remaining_keys = (
    df_products
    .select("brand", "id")
    .subtract(
        df_products
        .filter(F.col("cost") < 310)
        .select("brand", "id")
    )
)

verified = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.stddev_samp("cost").over(w).alias("supplier_id_stddev_samp_cost"),
        F.dense_rank().over(w2).alias("stock_qty_dense_rank"),
        F.avg("cost").over(w3).alias("category_avg_cost"),
    )
)

enriched = (
    metrics
    .withColumn("supplier_id_max_supplier_id_stddev_samp_cost", F.max("supplier_id_stddev_samp_cost").over(w4))
    .withColumn("category_avg_cost_percent_rank", F.percent_rank().over(w5))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.rating"), F.col("b.city"))
)

with_dims_products = (
    with_dims
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.country"))
)

with_windows = (
    with_dims_products
    .withColumn("supplier_id_stddev_samp_cost_rank", F.rank().over(w6))
    .withColumn("supplier_id_max_supplier_id_stddev_samp_cost_dense_rank", F.dense_rank().over(w7))
    .withColumn("supplier_id_sum_stock_qty", F.sum("stock_qty").over(w))
    .withColumn("corr_stock_qty_price", F.corr("stock_qty", "price").over(w8))
)

joined = (
    with_windows
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.supplier_name"), F.col("s.city").alias("supplier_city"))
)

result = (
    joined
    .withColumn("city_stddev_samp_supplier_id_sum_stock_qty", F.stddev_samp("supplier_id_sum_stock_qty").over(w8))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w9)), 4))
    .withColumn("corr_stock_qty_price_dense_rank", F.dense_rank().over(w10))
    .withColumn("supplier_id_max_supplier_id_stddev_samp_cost_dense_rank_rank", F.rank().over(w11))
    .withColumn("brand_sum_supplier_id_sum_stock_qty", F.sum("supplier_id_sum_stock_qty").over(w12))
)
