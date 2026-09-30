from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")

scored = (
    df_inventory
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w2))
)

result = (
    scored
    .groupBy("product_id")
    .agg(
        F.max("last_known_quantity").alias("max_last_known_quantity"),
        F.round(F.avg("last_known_quantity"), 2).alias("avg_last_known_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("max_last_known_quantity") < 0)
    .orderBy("product_id")
)
