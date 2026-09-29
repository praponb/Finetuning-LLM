from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("product_name").like("%Sho%")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.units") < 11)
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
    )
)
