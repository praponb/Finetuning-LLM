from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue")

result = (
    df_store_sales
    .select("id", "store_id", "revenue", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "store_id", "revenue")
)
