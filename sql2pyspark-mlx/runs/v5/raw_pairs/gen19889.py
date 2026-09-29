from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .filter(F.col("amount") >= 1340)
    .select("id", "amount", "status", F.last("status").over(w).alias("last_status"))
)
