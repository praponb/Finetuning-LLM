from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2024-03-21")))
    .select("*", F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"))
)

result = (
    base
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.price"), F.col("b.launch_date"))
)
