from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Acme") & (F.col("price") > 360))
    .select("tags", "brand", "launch_date")
    .orderBy("launch_date")
)
