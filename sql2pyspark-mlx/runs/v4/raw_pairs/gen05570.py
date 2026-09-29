from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1800)
    .select("id", "product_id", "revenue", F.row_number().over(w).alias("revenue_row"))
    .orderBy(F.col("revenue_row").desc())
)
