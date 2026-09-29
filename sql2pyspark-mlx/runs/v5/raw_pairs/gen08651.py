from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date")

result = (
    df_store_sales
    .select("id", "product_id", "sale_date", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "sale_date")
    .orderBy(F.col("product_id").desc())
)
