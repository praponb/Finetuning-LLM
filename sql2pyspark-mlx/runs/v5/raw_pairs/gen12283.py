from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") < "2025-02-13") & (F.col("stock_qty") > 317))
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("price")).otherwise(0)).alias("electronics_price"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("electronics_price").desc(), "row_count")
)
