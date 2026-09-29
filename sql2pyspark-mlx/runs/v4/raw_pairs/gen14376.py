from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("event_count").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"), F.col("c.quantity"))
)

result = (
    with_last
    .groupBy("category", F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_rows"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") > 1)
    .withColumn("event_count_dense_rank", F.dense_rank().over(w2))
    .orderBy("event_count")
)
