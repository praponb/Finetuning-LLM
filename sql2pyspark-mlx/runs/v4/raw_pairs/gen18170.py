from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-12-25").cast("date")) & F.col("order_id").isNotNull() & (F.col("order_id") == 6))
    .withColumn("status_lower", F.lower("status"))
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
