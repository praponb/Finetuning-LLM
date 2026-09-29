from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("revenue") >= 1700)
    .select("id", "store_id", "units", "sale_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "store_id", "units", "sale_date")
)
