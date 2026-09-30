from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty", "id")
w2 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w3 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("stock_qty") == 327))
    .select(
        "*",
        F.upper("brand").alias("brand_upper"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.avg("stock_qty").over(w3).alias("stock_qty_avg_2"),
    )
)
