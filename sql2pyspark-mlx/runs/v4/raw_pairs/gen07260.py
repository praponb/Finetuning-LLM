from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .select("id", "customer_id", "amount", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "customer_id", "amount")
    .orderBy(F.col("customer_id").desc())
)
