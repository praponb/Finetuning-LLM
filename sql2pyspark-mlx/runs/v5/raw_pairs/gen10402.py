from pyspark.sql import functions as F

filtered_products = df_products.filter((F.col("brand") != "Acme") & (F.size("tags") >= 1))

agg = (
    filtered_products
    .groupBy("brand", "category")
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = (
    agg
    .select("brand", "category", "last_launch_date")
    .orderBy("brand", "category")
    .limit(20)
)
