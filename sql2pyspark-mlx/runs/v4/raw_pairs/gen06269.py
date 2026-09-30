from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id")

result = (
    df_store_sales
    .filter(F.col("revenue") < 2700)
    .select("id", "store_id", "units", F.row_number().over(w).alias("units_row"))
)
