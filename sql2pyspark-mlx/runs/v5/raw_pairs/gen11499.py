from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([4, 8, 11]))
    .select("id", "product_id", "revenue", F.rank().over(w).alias("revenue_rank"))
    .orderBy("product_id")
)
