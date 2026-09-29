from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w_running = Window.partitionBy("country").orderBy("rating_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("rating_int").desc())
w_recent = Window.partitionBy("country").orderBy("rating_bucket", "id")
w5 = Window.partitionBy("country")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .withColumn("country_supplier_name_key", F.concat_ws("-", "country", "supplier_name"))
    .withColumn("city_filled", F.coalesce("city", F.lit("OTHER")))
)

base = (
    valid
    .filter((~F.col("city").eqNullSafe(F.lit("Tokyo"))) & F.col("city").isNotNull())
    .select("*", F.col("rating").cast("int").alias("rating_int"))
)

kept = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.size(F.col("ch.tags")) >= 1), "left_anti")
)

scored = (
    kept
    .filter(F.col("rating_int") > 49)
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "country_supplier_name_key",
        "city_filled",
        F.ntile(4).over(w).alias("rating_bucket"),
        F.sum("rating").over(w_running).alias("cumulative_rating"),
        F.rank().over(w3).alias("rating_int_rank"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "rating",
        "country_supplier_name_key",
        "rating_int_rank",
        F.lead("rating_int_rank").over(w_recent).alias("next_rating_int_rank"),
        F.lag("rating_int_rank").over(w_recent).alias("prev_rating_int_rank"),
        F.lead("rating").over(w_recent).alias("next_rating"),
        F.lag("rating").over(w_recent).alias("prev_rating"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.stddev_samp("rating").over(w5).alias("country_stddev_samp_rating"),
    )
)

filtered = (
    enriched
    .filter(F.col("event_seq").isNotNull())
    .select(
        "*",
        F.greatest("next_rating_int_rank", "rating").alias("greatest_next_rating_int_rank_rating"),
    )
)

report = (
    filtered
    .select(
        "id",
        "rating",
        "country_supplier_name_key",
        "next_rating_int_rank",
        "prev_rating_int_rank",
        "next_rating",
        "prev_rating",
        "country_stddev_samp_rating",
    )
    .orderBy(F.col("prev_rating_int_rank").desc_nulls_first())
)

result = (
    report
    .select(
        "country_supplier_name_key",
        "next_rating_int_rank",
        "next_rating",
        "prev_rating",
        "country_stddev_samp_rating",
    )
)
