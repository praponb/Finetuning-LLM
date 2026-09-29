from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc_nulls_last(), "id")

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("stock_qty") <= 228))
    .withColumn("stock_qty_plus_cost", (F.col("stock_qty") + F.col("cost")))
    .withColumn("cost_plus_stock_qty", (F.col("cost") + F.col("stock_qty")))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("NONE")))
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
