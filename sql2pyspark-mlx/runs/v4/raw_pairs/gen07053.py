from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)
