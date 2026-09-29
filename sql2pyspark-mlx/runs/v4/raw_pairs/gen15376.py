from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-08").cast("date"))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
