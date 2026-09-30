from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc(), "id")

result = df_store_sales.select("id", "units", F.row_number().over(w).alias("row_num"))
