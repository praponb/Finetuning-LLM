from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([6, 10, 11]))
    .select("id", "status", "order_id", "cost", F.ntile(4).over(w).alias("cost_ntile"))
    .orderBy("cost", "order_id")
)
