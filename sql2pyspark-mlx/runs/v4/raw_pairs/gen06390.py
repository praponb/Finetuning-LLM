from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .filter(F.col("order_id").isin([3, 6, 16, 19]))
    .select("id", "status", "cost", "order_id", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "status", "cost", "order_id")
)
