from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity_bucket")

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 9]))
    .groupBy(
        F.when(F.col("quantity") >= 194, "high").when(F.col("quantity") >= 105, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
)
