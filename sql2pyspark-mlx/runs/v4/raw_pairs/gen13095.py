from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc(), "id")
w_part = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")
w4 = Window.partitionBy("product_id").orderBy("revenue", "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-06-26").cast("date")) & (F.col("sale_date") < "2025-07-01"))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 3))
)

result = (
    filtered
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w_part))
    .withColumn("next_revenue", F.lead("revenue", 1).over(w3))
    .withColumn("revenue_bucket", F.ntile(10).over(w4))
)
