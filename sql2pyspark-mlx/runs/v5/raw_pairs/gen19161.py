from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "*",
        F.substring("city", 1, 2).alias("city_prefix"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "country",
        "city",
        "rating",
        "city_prefix",
        F.avg("rating").over(w).alias("rating_avg_2"),
        F.first("rating", ignorenulls=True).over(w2).alias("first_known_rating"),
    )
)
