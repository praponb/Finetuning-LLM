from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Bac%"))
    .groupBy("brand", F.month("launch_date").alias("launch_month"))
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("price")).otherwise(0)).alias("globex_price"),
        F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0)).alias("acme_count"),
    )
)
