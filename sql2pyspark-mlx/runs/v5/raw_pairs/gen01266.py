from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_first())

cleaned = (
    df_store_sales
    .filter(F.col("revenue") >= 2700)
    .select(
        "*",
        F.when(F.col("units") >= 40, "High").when(F.col("units") >= 20, "Medium").otherwise("Low").alias("units_level"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "sale_date",
        "units",
        F.ntile(10).over(w).alias("revenue_bucket"),
        F.cume_dist().over(w2).alias("units_cume_dist"),
    )
)
