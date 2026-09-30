from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.partitionBy("city")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .withColumn("city_filled", F.coalesce("city", F.lit("Unknown")))
    .withColumn("country_lower", F.lower("country"))
)

result = (
    filtered
    .select(
        "*",
        F.lead("rating").over(w).alias("next_rating"),
        F.lag("rating").over(w).alias("prev_rating"),
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
    )
)
