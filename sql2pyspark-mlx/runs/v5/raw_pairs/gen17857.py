from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country", "city").orderBy("rating")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("rating_whole").desc(), "id")
w4 = Window.partitionBy("country").orderBy(F.col("row_seq").desc())
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("country", "city").orderBy(F.col("rating_whole").desc_nulls_last())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.ceil("rating").alias("rating_whole"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

metrics = (
    base
    .filter(F.col("supplier_name").like("%City%"))
    .withColumn("rating_rank", F.rank().over(w_hist))
    .withColumn("last_known_rating_whole", F.last("rating_whole", ignorenulls=True).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

remaining_keys = (
    metrics
    .select("city", "id")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating").between(1.8, 2.2))
        .select("city", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "id",
        "supplier_name",
        "rating",
        "rating_rank",
        "row_seq",
        F.dense_rank().over(w4).alias("row_seq_dense_rank"),
        F.count("rating").over(w5).alias("country_count_rating"),
        F.sum("last_known_rating_whole").over(w6).alias("city_sum_last_known_rating_whole"),
        F.dense_rank().over(w7).alias("rating_whole_dense_rank"),
        F.min("rating_rank").over(w6).alias("city_min_rating_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("city_min_rating_rank") >= 8, "LARGE").otherwise("SMALL").alias("city_min_rating_rank_tier"),
        F.when(F.col("country_count_rating") >= 14, "HIGH").otherwise("LOW").alias("country_count_rating_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("country_count_rating").alias("total_country_count_rating"),
        F.percentile_approx("city_sum_last_known_rating_whole", [0.25, 0.5, 0.75]).alias("city_sum_last_known_rating_whole_quartiles"),
        F.sum("row_seq_dense_rank").alias("total_row_seq_dense_rank"),
    )
    .filter(F.col("total_row_seq_dense_rank") >= 0)
    .orderBy(F.col("country_count_rating_tier").desc())
)
