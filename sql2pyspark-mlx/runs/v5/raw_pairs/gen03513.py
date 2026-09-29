from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("pct_of_total", F.round(((F.col("total_quantity") * 100) / F.sum("total_quantity").over(w)), 2))
)
