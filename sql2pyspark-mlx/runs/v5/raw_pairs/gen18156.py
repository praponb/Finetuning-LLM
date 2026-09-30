from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") < 420) & (F.year("launch_date") >= 2024) & (F.col("price") > 810))
    .groupBy("brand")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
    .orderBy(F.col("price_range").desc(), "brand")
    .limit(3)
)
