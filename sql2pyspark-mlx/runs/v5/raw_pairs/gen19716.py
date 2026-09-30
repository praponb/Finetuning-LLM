from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

base = (
    df_store_sales
    .filter(F.col("revenue") < 3400)
    .select(
        "*",
        F.round("revenue", 2).alias("revenue_rounded"),
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
