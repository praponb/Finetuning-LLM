from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id")

result = (
    df_orders
    .filter(F.col("customer_id").isin([8, 30, 37]))
    .select("id", "status", "discount", F.row_number().over(w).alias("rn"))
)
