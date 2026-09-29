from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("revenue") <= 1800)
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
        F.quarter("sale_date").alias("sale_date_quarter"),
    )
)

result = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "product_id", "sale_date", "units", "sale_date_quarter")
)
