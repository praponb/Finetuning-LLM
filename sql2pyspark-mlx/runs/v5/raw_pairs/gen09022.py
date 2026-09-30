from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country", "city").orderBy("rating_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy("rating_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Singapore")))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.trim("country").alias("clean_country"),
        F.concat("supplier_name", F.lit(", "), "country").alias("supplier_name_country"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("city").isNotNull())
    .select("*", F.floor("rating").alias("rating_whole"))
)

cleaned = (
    valid
    .filter(F.col("city").isNotNull())
    .withColumn("country_city", F.concat("country", F.lit(", "), "city"))
)

with_windows = (
    cleaned
    .filter(F.col("country").isin(["JP", "US", "UK"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "city_filled",
        "rating_whole",
        F.avg("rating_whole").over(w).alias("country_avg_rating_whole"),
        F.sum("rating_whole").over(w2).alias("cumulative_rating_whole"),
        F.last("rating", ignorenulls=True).over(w_running).alias("last_known_rating"),
    )
)

filtered_suppliers = (
    with_windows
    .filter(F.col("rating_whole").isNull())
    .select(
        "id",
        "city",
        "rating",
        "city_filled",
        "rating_whole",
        "country_avg_rating_whole",
        "cumulative_rating_whole",
        "last_known_rating",
        F.greatest("rating", "last_known_rating").alias("greatest_rating_last_known_rating"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled_2"),
    )
)

kept = (
    filtered_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price").between(560, 720)), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "rating",
        "city_filled",
        "rating_whole",
        "country_avg_rating_whole",
        "cumulative_rating_whole",
        "greatest_rating_last_known_rating",
        "city_filled_2",
    )
    .orderBy(F.col("rating").asc_nulls_first())
)
