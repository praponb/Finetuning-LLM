from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .select("id", "customer_id", "amount", "status", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "amount", "status")
    .orderBy(F.col("amount").desc())
)
