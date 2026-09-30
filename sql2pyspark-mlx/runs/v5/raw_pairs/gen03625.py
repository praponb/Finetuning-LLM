from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country").orderBy(F.col("rating_filled").desc(), "id")
w_grp = Window.partitionBy("country", "city").orderBy("rating_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("rating_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Berlin")))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
    .withColumn("city_clean", F.coalesce("city", F.lit("DIRECT")))
)

result = (
    valid
    .filter(F.col("rating") > 3.3)
    .select(
        "id",
        "country",
        "city",
        "rating_filled",
        "city_clean",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.ntile(10).over(w2).alias("rating_filled_bucket"),
        F.first("rating_filled", ignorenulls=True).over(w_grp).alias("first_known_rating_filled"),
        F.last("city", ignorenulls=True).over(w4).alias("last_known_city"),
    )
)
