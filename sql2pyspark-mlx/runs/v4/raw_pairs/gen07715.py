from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_store_sales
    .withColumn("revenue_dense_rank", F.dense_rank().over(w))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4))
    .withColumn("last_known_units", F.last("units", ignorenulls=True).over(w3))
)

result = (
    scored
    .groupBy("store_id", "product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("distinct_product_ids").desc())
)
