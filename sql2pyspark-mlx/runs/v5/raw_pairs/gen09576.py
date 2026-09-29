from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

ranked = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "quantity", "unit_price")
)

result = (
    ranked
    .groupBy("product_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.count("*").alias("event_count"),
    )
    .orderBy("product_id")
)
