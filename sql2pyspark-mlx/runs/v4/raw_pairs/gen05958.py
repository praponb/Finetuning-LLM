from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .filter(F.col("launch_date") <= "2025-10-21")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "supplier_id", "stock_qty", "launch_date")
    .orderBy("stock_qty")
)
