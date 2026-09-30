from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("event_count").desc())

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-07-06").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("WEEK", "last_restock_plus_months").alias("week_start"),
        "store_id",
        "product_id",
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids_2"),
        F.count("*").alias("event_count"),
    )
    .withColumn("event_count_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("distinct_product_ids").desc())
)
