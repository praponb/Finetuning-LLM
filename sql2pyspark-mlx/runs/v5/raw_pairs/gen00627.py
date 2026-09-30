from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2024-07-01")
    .select("id", "order_id", "shipped_date", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "order_id", "shipped_date")
)
