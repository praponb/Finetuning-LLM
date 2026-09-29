from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("id", "customer_id", "order_date", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("order_date").desc(), F.col("id").desc())
)
