from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .select("id", "revenue", F.percent_rank().over(w).alias("revenue_pct_rank"))
    .orderBy("revenue")
)
