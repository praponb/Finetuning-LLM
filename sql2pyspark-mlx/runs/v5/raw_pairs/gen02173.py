from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Globex"]))
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("pct_of_total", F.round(((F.col("total_cost") * 100) / F.sum("total_cost").over(w)), 2))
)
