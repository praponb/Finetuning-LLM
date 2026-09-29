from pyspark.sql import functions as F

with_dims = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.launch_date") >= "2024-05-01")
    .select("a.*", F.col("p.category"))
)

result = (
    with_dims
    .select("id", "store_id", "sale_date", "revenue")
    .orderBy(F.col("store_id").desc_nulls_last())
)
