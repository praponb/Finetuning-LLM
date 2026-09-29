from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper("product_name").like("%IRE%"))
    .groupBy("brand")
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("cost")).otherwise(0)).alias("globex_cost"),
    )
)
