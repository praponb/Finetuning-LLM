from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date_minus_63d", "id")

cleaned = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-10-17")))
    .select("*", F.date_sub("order_date", 63).alias("order_date_minus_63d"))
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
