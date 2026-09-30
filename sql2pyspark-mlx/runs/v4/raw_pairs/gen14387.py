from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy(F.col("units").desc())

filtered = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2024-02-07"))) & F.col("product_id").isin([1, 2, 10]))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.round("units", 0).alias("units_rounded"),
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .withColumn("store_id_sum_units_rounded", F.sum("units_rounded").over(w))
    .withColumn("units_cume_dist", F.cume_dist().over(w2))
)
