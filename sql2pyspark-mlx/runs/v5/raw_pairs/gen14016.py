from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.lit("store_sale").alias("origin"), "sale_date")
    .union(
        df_products
        .filter(F.col("cost") < 310)
        .select(F.lit("product").alias("origin"), "launch_date")
    )
    .distinct()
    .orderBy("sale_date")
)
