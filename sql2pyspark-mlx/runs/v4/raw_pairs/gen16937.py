from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select("id", "channel", "amount", "store_id", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "channel", "amount", "store_id")
    .orderBy(F.col("amount").desc())
)
