from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2024-03-15")))
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
)

result = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
