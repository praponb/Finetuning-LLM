from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last(), "id")

filtered = (
    df_stores
    .filter(F.col("store_name").like("%River%"))
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        F.length("region").alias("region_length"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
