from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("cost") < 260))
    .select(
        "*",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

result = (
    valid
    .withColumn("prev_price", F.lag("price", 1).over(w))
    .withColumn("running_cost", F.sum("cost").over(w2))
    .withColumn("prev_cost", F.lag("cost", 1).over(w))
)
