from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.countDistinct("stock_qty").alias("unique_suppliers"),
    )
    .filter(F.col("avg_price") < 72)
    .orderBy(F.col("avg_price").desc(), "brand")
    .limit(5)
)
