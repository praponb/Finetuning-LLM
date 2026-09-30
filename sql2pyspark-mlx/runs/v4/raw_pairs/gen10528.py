from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id")
w2 = Window.partitionBy("category").orderBy("price", "id")

cleaned = (
    df_products
    .filter(F.col("category").isin(["Fashion", "Electronics", "Grocery"]))
    .withColumn("cost_minus_stock_qty", (F.col("cost") - F.col("stock_qty")))
    .withColumn("launch_date_month", F.month("launch_date"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("N/A")))
)

result = (
    cleaned
    .withColumn("prev_cost_minus_stock_qty", F.lag("cost_minus_stock_qty", 1).over(w))
    .withColumn("prev_launch_date_month", F.lag("launch_date_month", 1).over(w2))
)
