from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = df_carts.select("id", "created_at", F.explode("attributes").alias("k", "v"))

result = (
    lines
    .withColumn("last_known_k", F.last("k", ignorenulls=True).over(w_running))
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w_running))
)
