from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("stock_qty") > 353)
    .select("id", "brand", "launch_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "brand", "launch_date")
)
