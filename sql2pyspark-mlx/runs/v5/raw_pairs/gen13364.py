from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") >= 2023)
    .groupBy("brand")
    .pivot("category", ["Books", "Home"])
    .agg(F.avg("cost"))
    .orderBy("brand")
)
