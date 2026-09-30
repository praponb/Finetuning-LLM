from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Electronics", "Sports"]))
    .select("launch_date", F.lit("product").alias("origin"))
    .union(
        df_store_sales
        .select("sale_date", F.lit("store_sale").alias("origin"))
    )
)
