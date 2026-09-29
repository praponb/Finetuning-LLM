from pyspark.sql import functions as F

base = df_products.filter((F.col("launch_date") > "2025-06-01") & (F.col("cost") < 570))

agg = (
    base
    .groupBy("brand")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.min("launch_date").alias("min_launch_date"),
    )
)

result = agg.orderBy("min_launch_date")
