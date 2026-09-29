from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("brand") == "Stark") & F.col("stock_qty").between(250, 345))
)

products_stats = (
    filtered_products
    .groupBy(
        F.when(F.col("brand").isin(["Umbrella", "Initech"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = products_stats.orderBy("brand_group", F.col("category_values").desc())
