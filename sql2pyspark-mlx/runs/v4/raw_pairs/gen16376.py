from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2025-06-01")
    .select("id", "store_id", "product_id", "revenue", F.rank().over(w).alias("rnk"))
    .orderBy("id")
)
