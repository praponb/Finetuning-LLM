from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_month")

result = (
    df_shipments
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
    .orderBy("total_cost")
)
