from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Lio%"))
    .select("*", F.substring("city", 1, 3).alias("city_prefix"))
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "country", "rating", "city_prefix")
)
