from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        F.when(F.col("brand").isin(["Globex", "Initech"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.max("price").alias("max_price"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
    .filter(F.col("max_price") < 1080)
    .select("category", "brand_group", "max_price", "brand_values")
    .orderBy("category")
)
