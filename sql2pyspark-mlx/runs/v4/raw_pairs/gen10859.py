from pyspark.sql import functions as F

src = df_inventory.filter(F.col("store_id").isin([1, 3, 4, 6]))

last_restock_quarter_summary = (
    src
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    last_restock_quarter_summary
    .filter(F.col("avg_quantity") > 71)
    .orderBy(F.col("last_restock_quarter").desc(), F.col("avg_quantity").desc())
)
