from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("rating") >= 3.6, "High").when(F.col("rating") >= 2.2, "Medium").otherwise("Low").alias("rating_band"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "rating", "rating_band")
)
