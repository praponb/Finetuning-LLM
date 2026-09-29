from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("cnt").desc())

result = (
    df_inventory
    .groupBy("store_id", F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .withColumn("cnt_rank", F.rank().over(w))
)
