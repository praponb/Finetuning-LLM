from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("country_length").desc_nulls_first())
w3 = Window.orderBy(F.col("country_length_rank").desc_nulls_first())
w4 = Window.partitionBy("country").orderBy("country_length_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("country_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("last_known_country_length_rank", "id")

filtered = (
    df_suppliers
    .filter(F.length("supplier_name") >= 13)
    .withColumn("country_length", F.length("country"))
    .withColumn("city_prefix", F.substring("city", 1, 2))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("country_length").over(w).alias("cumulative_country_length"),
        F.rank().over(w2).alias("country_length_rank"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w3).alias("country_length_rank_percent_rank"),
        F.last("country_length_rank", ignorenulls=True).over(w4).alias("last_known_country_length_rank"),
    )
)

result = (
    enriched
    .select(
        "*",
        F.sum("country_length_rank").over(w5).alias("running_country_length_rank"),
        F.lag("cumulative_country_length", 1, 0).over(w6).alias("prev_cumulative_country_length"),
    )
)
