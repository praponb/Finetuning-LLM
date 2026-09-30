from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_shipments
    .withColumn("last7_max_cost", F.max("cost").over(w))
    .withColumn("last7_avg_cost", F.avg("cost").over(w))
    .withColumn("last7_count_cost", F.count("cost").over(w))
    .withColumn("last7_sum_cost", F.sum("cost").over(w))
)
