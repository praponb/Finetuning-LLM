from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("city", "country").orderBy(F.col("credit_limit").asc_nulls_last(), "id")
w3 = Window.partitionBy("country").orderBy("signup_date", "id")

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-04-15"))) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.concat("city", F.lit(", "), "name").alias("city_name"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group_2", F.row_number().over(w3))
    .filter(F.col("rank_in_group_2") == 1)
)

kept_keys = (
    latest
    .select("segment")
    .intersect(
        df_customers
        .filter(F.col("country") == "DE")
        .select("segment")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

remaining_keys = (
    reconciled
    .select("id", "segment")
    .intersectAll(
        df_customers
        .filter(F.year("signup_date") == 2025)
        .select("id", "segment")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .groupBy(
        F.when(F.col("credit_limit") >= 14600, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("credit_limit_per_smb"),
    )
    .filter(F.col("credit_limit_per_smb") > 0)
)

result = (
    grouped
    .select("credit_limit_tier", "bangkok_events", "credit_limit_quartiles")
    .orderBy(F.col("bangkok_events").asc_nulls_last())
)
