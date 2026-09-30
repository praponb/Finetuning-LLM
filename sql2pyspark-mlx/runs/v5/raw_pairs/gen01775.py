from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue_per_unit").asc_nulls_last(), "id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-05-24").cast("date")) & (F.col("product_id") == 4))
    .select(
        "*",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "sale_date")
)
