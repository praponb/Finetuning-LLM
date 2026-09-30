from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_month")

result = (
    df_stores
    .filter(F.month("opened_date").isin([3, 6, 8]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
)
