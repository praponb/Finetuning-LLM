from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")
w2 = Window.partitionBy("country")

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("signup_date") <= "2024-03-01"))
    .select(
        "*",
        (F.col("age") * F.col("credit_limit")).alias("age_times_credit_limit"),
        F.round("credit_limit", 2).alias("credit_limit_rounded"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    deduped
    .select("id", "city")
    .intersectAll(
        df_customers
        .filter(F.col("city").isin(["Singapore", "New York", "Bangkok"]))
        .select("id", "city")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.avg("credit_limit_rounded").over(w2).alias("country_avg_credit_limit_rounded"),
    )
)

result = (
    with_windows
    .select(
        "name",
        "email",
        "country",
        "city",
        "segment",
        "credit_limit",
        "age",
        "age_times_credit_limit",
        "credit_limit_rounded",
    )
    .orderBy(F.col("segment").desc_nulls_first(), F.col("credit_limit_rounded").desc())
)
