from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")

latest = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select("id", "order_id", "carrier", "shipped_date", "delivered_date", "cost")
    .orderBy(F.col("order_id").asc_nulls_last(), F.col("carrier").desc())
)
