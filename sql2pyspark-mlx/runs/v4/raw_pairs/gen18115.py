from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.partitionBy("city", "country").orderBy("rating", "id")
w3 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("rating", "id")
w8 = Window.partitionBy("city", "country").orderBy(F.col("prev_rating_share").asc_nulls_last())
w9 = Window.partitionBy("city", "country")
w10 = Window.partitionBy("country", "city")

base = (
    df_suppliers
    .filter(F.col("rating").between(2.6, 3.5))
    .select(
        "*",
        F.concat("city", F.lit(" - "), "supplier_name").alias("city_supplier_name"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

with_windows = (
    base
    .withColumn("prev_rating", F.lag("rating", 2).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

cleaned = (
    with_windows
    .filter(F.col("rating").between(2.2, 2.3))
    .withColumn("country_title", F.initcap(F.trim("country")))
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("row_seq").over(w3).alias("row_seq_avg_6"),
        F.round((F.col("rating") / F.sum("rating").over(w4)), 4).alias("rating_share"),
        F.round((F.col("prev_rating") / F.sum("prev_rating").over(w5)), 4).alias("prev_rating_share"),
        F.last("rating", ignorenulls=True).over(w6).alias("last_known_rating"),
        F.lead("row_seq", 1).over(w7).alias("next_row_seq"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

kept_keys = (
    deduped
    .select("city", "id")
    .subtract(
        df_suppliers
        .filter(F.col("rating") < 2.9)
        .select("city", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("rating_share_2", F.round((F.col("rating") / F.sum("rating").over(w9)), 4))
    .withColumn("prev_rating_share_2", F.round((F.col("prev_rating") / F.sum("prev_rating").over(w10)), 4))
)

result = (
    enriched
    .select(
        "city",
        F.col("prev_rating_share_2").alias("PREV_RATING_SHARE_2"),
        "prev_rating",
    )
    .unpivot(["city"], ["PREV_RATING_SHARE_2", "prev_rating"], "measure", "value")
    .filter(F.col("value").isNotNull())
)
