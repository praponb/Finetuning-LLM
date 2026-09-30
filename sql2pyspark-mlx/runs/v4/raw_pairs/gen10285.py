from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("country")

with_windows = (
    df_suppliers
    .withColumn("running_rating", F.sum("rating").over(w))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w2))
    .withColumn("next_rating", F.lead("rating", 1).over(w3))
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w4))
)

metrics = (
    with_windows
    .withColumn("city_min_running_rating", F.min("running_rating").over(w5))
    .withColumn("country_count_rating", F.count("rating").over(w6))
)

valid = (
    metrics
    .filter(F.col("last_known_rating") > 2.9)
    .select(
        "id",
        "supplier_name",
        "city",
        "running_rating",
        "last_known_rating",
        "country_count_rating",
        (F.col("running_rating") - F.col("rating")).alias("running_rating_minus_rating"),
        F.round((F.col("city_min_running_rating") / F.col("rating")), 2).alias("city_min_running_rating_per_rating"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "city",
        "running_rating",
        "last_known_rating",
        "country_count_rating",
        "running_rating_minus_rating",
        "city_min_running_rating_per_rating",
        "city_filled",
    )
)
