from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2024-04-18")
    .groupBy("brand")
    .pivot("category", ["Grocery", "Books", "Fashion", "Electronics"])
    .agg(F.avg("price"))
    .orderBy("brand")
)
