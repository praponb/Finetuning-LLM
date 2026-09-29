from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("country_upper", F.upper("country"))
    .withColumn("city_clean", F.coalesce("city", F.lit("NONE")))
)

result = (
    valid
    .select(
        "id",
        "country",
        "country_upper",
        "city_clean",
        F.count("rating").over(w_hist).alias("running_count_rating"),
        F.avg("rating").over(w_hist).alias("running_avg_rating"),
        F.sum("rating").over(w_hist).alias("running_sum_rating"),
        F.sum("rating").over(w_hist).alias("running_rating"),
    )
)
