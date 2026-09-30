from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .groupBy("brand")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.max("stock_qty").alias("max_stock_qty"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
    )
    .filter(F.col("initech_count") > 1)
    .drop("initech_count")
    .orderBy(F.col("max_stock_qty").desc())
)
