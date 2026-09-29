from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating_whole", "id")
w2 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("rating_whole", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Tokyo%"))
    .withColumn("country_prefix", F.substring("country", 1, 3))
    .withColumn("rating_whole", F.ceil("rating"))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    ranked
    .select(
        "*",
        F.avg("rating_whole").over(w2).alias("rating_whole_avg_3"),
        F.avg("rating").over(w3).alias("rating_avg_3"),
        F.avg("rating_whole").over(w4).alias("rating_whole_avg_5"),
    )
)

result = scored.select("id", "country", "rating_whole_avg_3", "rating_whole_avg_5")
