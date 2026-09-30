from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("revenue").between(2200, 3200))
    .select("id", "store_id", "units", "revenue", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "store_id", "units", "revenue")
)
