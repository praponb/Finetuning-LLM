from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating_filled", "id")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("brand").orderBy("prev_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.upper("country").alias("country_upper"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

scored = (
    base
    .filter(F.col("city") == "Singapore")
    .select(
        "id",
        "rating",
        "rating_filled",
        "country_upper",
        F.lag("rating_filled").over(w).alias("prev_rating_filled"),
        F.lag("rating").over(w).alias("prev_rating"),
        F.row_number().over(w).alias("row_seq"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand"))
)

with_windows = (
    latest_match
    .select(
        "*",
        F.sum("prev_rating").over(w_grp).alias("running_sum_prev_rating"),
        F.min("prev_rating").over(w_grp).alias("running_min_prev_rating"),
        F.count("prev_rating").over(w_grp).alias("running_count_prev_rating"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "rating",
        "prev_rating",
        "row_seq",
        "running_rating",
        "brand",
        "running_sum_prev_rating",
        "running_min_prev_rating",
        "running_count_prev_rating",
        F.when(F.col("brand").rlike("^(Umbrella|Initech)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("brand_zone"),
    )
)
