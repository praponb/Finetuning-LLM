from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 5, 6]))
    .select("id", "product_id", "revenue", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
)
