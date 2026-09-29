from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("store_id") == 1)
    .select("id", "store_id", "order_date", F.ntile(4).over(w).alias("bucket"))
    .orderBy(F.col("store_id").desc())
)
