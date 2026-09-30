from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").asc_nulls_last())
w2 = Window.orderBy("stock_qty", "id")
w3 = Window.partitionBy("brand").orderBy("stock_qty", "id")
w4 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_products
    .withColumn("price_dense_rank", F.dense_rank().over(w))
    .withColumn("next_cost", F.lead("cost", 1).over(w2))
    .withColumn("prev_stock_qty", F.lag("stock_qty", 2, 0).over(w3))
    .withColumn("cumulative_price", F.sum("price").over(w4))
)

result = (
    enriched
    .groupBy("brand")
    .agg(
        F.percentile_approx("next_cost", 0.95).alias("next_cost_p95"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count_if(F.col("brand") == "Stark").alias("stark_events"),
        F.countDistinct("category").alias("distinct_categories"),
    )
)
