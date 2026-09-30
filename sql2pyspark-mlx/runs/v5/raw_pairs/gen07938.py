from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_first(), "id")

valid = (
    df_store_sales
    .filter(F.col("units") >= 35)
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
