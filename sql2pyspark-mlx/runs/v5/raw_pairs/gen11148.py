from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "London", "New York"]))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.trim("city").alias("clean_city"),
        F.length("country").alias("country_length"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
