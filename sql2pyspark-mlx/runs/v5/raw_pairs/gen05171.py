from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("price_tier").orderBy(F.col("corr_cost_price").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.revenue").alias("store_sale_revenue"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("price") >= 1250, "HIGH").otherwise("LOW").alias("price_tier"),
        "supplier_id",
        "brand",
    )
    .agg(
        F.corr("cost", "price").alias("corr_cost_price"),
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("stock_qty")).otherwise(0)).alias("acme_stock_qty"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .withColumn("corr_cost_price_dense_rank", F.dense_rank().over(w2))
)
