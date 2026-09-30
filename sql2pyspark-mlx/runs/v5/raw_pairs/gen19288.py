from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc())
w2 = Window.orderBy("unit_price", "id")
w3 = Window.partitionBy("next_quantity_tier").orderBy(F.col("avg_quantity_rank").desc())

scored = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        F.rank().over(w).alias("quantity_rank"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("next_quantity") >= 7, "HIGH").otherwise("LOW").alias("next_quantity_tier"),
        F.when(F.col("quantity_rank") >= 7, "LARGE").otherwise("SMALL").alias("quantity_rank_tier"),
    )
    .agg(
        F.percentile_approx("quantity_rank", 0.95).alias("quantity_rank_p95"),
        F.count("*").alias("cnt"),
        F.round(F.avg("quantity_rank"), 2).alias("avg_quantity_rank"),
    )
    .withColumn("avg_quantity_rank_rank", F.rank().over(w3))
)
