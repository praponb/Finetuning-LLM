from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "store"]))
    .select("id", "status", "amount", F.row_number().over(w).alias("rn"))
    .orderBy("rn")
)
