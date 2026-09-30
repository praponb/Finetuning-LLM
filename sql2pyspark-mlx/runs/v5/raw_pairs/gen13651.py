from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty_adjusted").asc_nulls_last())

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-06-08").cast("date")) & F.col("brand").isNotNull())
    .withColumn("cost_per_stock_qty", (F.col("cost") / F.nullif("stock_qty", F.lit(0))))
    .withColumn("stock_qty_adjusted", F.round((F.col("stock_qty") * 0.85), 2))
)

result = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "brand", "price", "launch_date", "supplier_id")
)
