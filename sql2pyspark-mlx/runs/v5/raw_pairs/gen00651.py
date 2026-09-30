from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue_per_unit").desc_nulls_last(), "id")

filtered = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
