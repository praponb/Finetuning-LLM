from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost_bucket")

result = (
    df_shipments
    .filter(F.col("order_id").isin([3, 5, 18, 21]))
    .groupBy(
        F.when(F.col("cost") >= 92, "high").when(F.col("cost") >= 35, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
    .orderBy(F.col("cost_bucket").desc())
)
