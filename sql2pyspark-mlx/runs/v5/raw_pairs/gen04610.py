from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .filter(F.col("stock_qty").isNotNull())
    .select("id", "brand", "launch_date", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .select("id", "brand", "launch_date")
)
