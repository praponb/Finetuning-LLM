from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(-6, -1)

scored = (
    df_inventory
    .select(
        "id",
        "store_id",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.sum("quantity").over(w2).alias("cumulative_quantity_2"),
        F.avg("quantity").over(w3).alias("quantity_avg_6_prior"),
    )
)

result = (
    scored
    .groupBy("store_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
        F.avg("quantity_avg_6_prior").alias("avg_quantity_avg_6_prior"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("cnt").desc())
)
