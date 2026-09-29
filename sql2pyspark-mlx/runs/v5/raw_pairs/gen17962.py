from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2025-04-01")
    .groupBy("category")
    .pivot("brand", ["Stark", "Umbrella"])
    .agg(F.avg("price"))
    .orderBy("category")
)
