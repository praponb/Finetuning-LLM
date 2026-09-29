from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

result = (
    df_orders
    .filter(F.col("amount") < 1380)
    .select("id", "customer_id", F.count("*").over(w).alias("customer_id_count"))
)
