from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-5, -1)

enriched = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.stddev_samp("rating").over(w).alias("country_stddev_samp_rating"),
        F.ntile(4).over(w2).alias("rating_bucket"),
        F.first("city", ignorenulls=True).over(w3).alias("first_known_city"),
        F.avg("rating").over(w4).alias("rating_avg_5_prior"),
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
    )
)

result = (
    enriched
    .select(
        "id",
        "country",
        "city",
        "rating",
        "country_stddev_samp_rating",
        "rating_avg_5_prior",
        "rating_share",
    )
    .orderBy(F.col("country_stddev_samp_rating").asc_nulls_last())
)
