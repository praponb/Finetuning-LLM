from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")
w2 = Window.partitionBy("country").orderBy("row_num", "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat("country", F.lit(" "), "supplier_name").alias("country_supplier_name"),
        F.trim("city").alias("clean_city"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.lead("rating").over(w2).alias("next_rating"),
        F.lag("rating").over(w2).alias("prev_rating"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_suppliers
        .filter(F.col("city").isin(["London", "Bangkok", "Chiang Mai"]))
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(
        F.when(F.col("row_seq") >= 8, "HIGH").otherwise("LOW").alias("row_seq_tier"),
        F.when(F.col("rating") >= 3.7, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.percentile_approx("next_rating", 0.9).alias("next_rating_p90"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
    )
    .filter(F.col("sg_events") < 1000)
    .orderBy(F.col("de_rows").asc_nulls_last())
)
