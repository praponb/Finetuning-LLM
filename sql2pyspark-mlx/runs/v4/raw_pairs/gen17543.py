from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("cost")

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("*", (F.col("cost") - F.col("stock_qty")).alias("cost_minus_stock_qty"))
)

result = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
