from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "city")
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("rating").desc())
w5 = Window.partitionBy("city", "country")
w6 = Window.orderBy(F.col("rating").desc_nulls_first())
w_ordered = Window.orderBy(F.col("country_count_rating").desc())
w8 = Window.orderBy(F.col("rating_rank").asc_nulls_first())
w9 = Window.orderBy(F.col("cumulative_rating").asc_nulls_first())

enriched = (
    df_suppliers
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w))
    .withColumn("country_count_rating", F.count("rating").over(w2))
    .withColumn("running_rating", F.sum("rating").over(w))
    .withColumn("cumulative_rating", F.sum("rating").over(w3))
)

remaining_keys = (
    enriched
    .select("country", "city")
    .subtract(
        df_suppliers
        .filter(F.col("rating").between(2.8, 3.9))
        .select("country", "city")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

enriched_suppliers = (
    reconciled
    .select(
        "id",
        "supplier_name",
        "rating",
        "country_count_rating",
        "running_rating",
        "cumulative_rating",
        F.rank().over(w4).alias("rating_rank"),
        F.count("rating").over(w5).alias("city_count_rating"),
    )
)

matched_keys = (
    enriched_suppliers
    .select("id")
    .intersect(
        df_suppliers
        .filter(F.col("country") == "SG")
        .select("id")
    )
)

verified = (
    enriched_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.rank().over(w6).alias("rating_rank_2"),
        F.percent_rank().over(w_ordered).alias("country_count_rating_percent_rank"),
        F.rank().over(w8).alias("rating_rank_rank"),
        F.dense_rank().over(w9).alias("cumulative_rating_dense_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("rating_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("rating_rank_tier"),
    )
    .agg(
        F.round(F.avg("country_count_rating_percent_rank"), 2).alias("avg_country_count_rating_percent_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1000)
)
