from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc())

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 6, 10]))
    .select("id", "units", F.rank().over(w).alias("rnk"))
)
