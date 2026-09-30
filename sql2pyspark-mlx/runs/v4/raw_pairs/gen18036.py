from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("category").orderBy("price", "id")
w3 = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_products
    .withColumn("corr_price_cost", F.corr("price", "cost").over(w))
    .withColumn("price_bucket", F.ntile(4).over(w2))
    .withColumn("cumulative_cost", F.sum("cost").over(w3))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("price") >= 1140, "HIGH").otherwise("LOW").alias("price_tier"),
        F.when(F.col("cumulative_cost") >= 3850, "PREMIUM").otherwise("STANDARD").alias("cumulative_cost_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("corr_price_cost"), 2).alias("avg_corr_price_cost"),
        F.count_if(F.col("brand") == "Stark").alias("stark_events"),
    )
    .orderBy(F.col("cumulative_cost_tier").desc())
)
