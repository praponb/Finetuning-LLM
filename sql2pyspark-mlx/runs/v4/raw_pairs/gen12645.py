from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select("id", "store_id", "amount", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
)
