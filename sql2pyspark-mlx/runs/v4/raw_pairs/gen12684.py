from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.launch_date")), "left_semi")
)

result = (
    not_flagged
    .select("id", "product_name", "price", "launch_date")
    .orderBy(F.col("launch_date").desc_nulls_first(), "id")
)
