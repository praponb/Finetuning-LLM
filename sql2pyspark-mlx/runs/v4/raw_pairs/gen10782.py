from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last())
w4 = Window.partitionBy("country", "city").orderBy("rating_dense_rank", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("city", "country")
w7 = Window.partitionBy("country", "city")
w8 = Window.partitionBy("city").orderBy(F.col("rating_dense_rank").desc())

with_windows = (
    df_suppliers
    .withColumn("cumulative_rating", F.sum("rating").over(w_part))
    .withColumn("country_avg_rating", F.avg("rating").over(w2))
    .withColumn("rating_dense_rank", F.dense_rank().over(w3))
)

scored = (
    with_windows
    .select(
        "*",
        F.avg("rating").over(w4).alias("rating_avg_3"),
        F.sum("rating_dense_rank").over(w5).alias("cumulative_rating_dense_rank"),
        F.avg("rating_dense_rank").over(w_hist).alias("city_avg_rating_dense_rank"),
        F.round((F.col("cumulative_rating") / F.sum("cumulative_rating").over(w7)), 4).alias("cumulative_rating_share"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
)

matched_keys = (
    deduped
    .select("country", "id")
    .exceptAll(
        df_suppliers
        .filter(F.length("supplier_name") >= 13)
        .select("country", "id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    reconciled
    .select("city", "country")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").isNotNull())
        .select("city", "country")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "cumulative_rating",
        "country_avg_rating",
        "rating_dense_rank",
        "rating_avg_3",
        "city_avg_rating_dense_rank",
    )
    .orderBy(F.col("city").desc(), F.col("rating_avg_3").desc())
)
