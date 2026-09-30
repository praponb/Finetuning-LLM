from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select("id", "customer_id", "order_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "customer_id", "order_date")
    .orderBy(F.col("order_date").desc(), F.col("customer_id").desc())
)
