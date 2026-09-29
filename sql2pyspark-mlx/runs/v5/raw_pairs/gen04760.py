from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units")

result = (
    df_store_sales
    .select("id", "product_id", "units", "sale_date", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "product_id", "units", "sale_date")
    .orderBy(F.col("sale_date").desc())
)
