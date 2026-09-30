from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select("id", "status", "order_date", F.row_number().over(w).alias("row_num"))
)
