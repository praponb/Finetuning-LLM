from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .select("id", "order_id", "cost", F.dense_rank().over(w).alias("cost_dense_rank"))
    .orderBy(F.col("id").desc())
)
