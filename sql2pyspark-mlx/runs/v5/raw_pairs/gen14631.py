from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(((F.col("launch_date") < "2023-12-01") & F.col("category").isin(["Home", "Fashion"])) | F.col("cost").between(550, 710)))
    .groupBy("brand")
    .agg(F.avg("cost").alias("avg_cost"))
)
