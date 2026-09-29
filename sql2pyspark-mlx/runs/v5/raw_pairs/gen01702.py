from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .select("id", "store_id", "revenue", F.rank().over(w).alias("revenue_rank"))
)
