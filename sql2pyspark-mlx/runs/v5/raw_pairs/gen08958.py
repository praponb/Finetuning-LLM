from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.countDistinct("price").alias("unique_categorys"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
    )
)
