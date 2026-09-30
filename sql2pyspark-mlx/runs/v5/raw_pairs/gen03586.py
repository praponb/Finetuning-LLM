from pyspark.sql import functions as F

base = df_products.filter((F.col("cost") <= 470) & (F.col("supplier_id") == 18))

agg = (
    base
    .groupBy("brand")
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = agg.select("brand", "min_launch_date")
