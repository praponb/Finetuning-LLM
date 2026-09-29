from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("carrier_group")

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["Flash", "Kerry"]), "priority").otherwise("other").alias("carrier_group"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
)
