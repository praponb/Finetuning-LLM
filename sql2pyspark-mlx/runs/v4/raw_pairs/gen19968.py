from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .select("id", "sale_date", F.row_number().over(w).alias("row_num"))
    .orderBy("sale_date")
)
