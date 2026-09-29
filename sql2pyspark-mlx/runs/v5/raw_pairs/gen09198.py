from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Electronics", "Home"]))
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .pivot("brand", ["Stark", "Globex"])
    .agg(F.max("cost"))
    .orderBy("launch_month")
)
