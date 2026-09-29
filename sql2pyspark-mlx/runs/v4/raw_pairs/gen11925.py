from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w3 = Window.orderBy("revenue", "id")

metrics = (
    df_store_sales
    .withColumn("cumulative_units", F.sum("units").over(w))
    .withColumn("prev_revenue", F.lag("revenue", 1).over(w2))
    .withColumn("revenue_bucket", F.ntile(4).over(w3))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("revenue") >= 3400, "HIGH").otherwise("LOW").alias("revenue_tier"),
    )
    .agg(
        F.round(F.avg("revenue_bucket"), 2).alias("avg_revenue_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") < 1)
    .orderBy(F.col("revenue_tier").desc())
)
