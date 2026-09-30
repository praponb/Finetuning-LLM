from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Umbrella")
    .groupBy("category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
)
