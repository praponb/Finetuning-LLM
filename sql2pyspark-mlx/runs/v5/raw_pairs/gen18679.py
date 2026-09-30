from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country")

scored = (
    df_customers
    .filter(F.col("country").isin(["TH", "UK", "JP"]))
    .select(
        "id",
        "email",
        "country",
        "segment",
        F.lead("age").over(w).alias("next_age"),
        F.lag("age").over(w).alias("prev_age"),
        F.lag("credit_limit").over(w).alias("prev_credit_limit"),
        F.row_number().over(w).alias("event_seq"),
    )
)

cleaned = (
    scored
    .filter((~F.col("segment").eqNullSafe(F.lit("Consumer"))) & (F.col("country") == "TH"))
    .withColumn("clean_email", F.trim("email"))
    .withColumn("event_seq_plus_next_age", (F.col("event_seq") + F.col("next_age")))
)

valid = (
    cleaned
    .filter(F.col("country").isin(["US", "TH"]))
    .select(
        "id",
        "email",
        "country",
        "segment",
        "prev_credit_limit",
        "event_seq",
        "clean_email",
        "event_seq_plus_next_age",
        (F.col("event_seq") * F.col("prev_credit_limit")).alias("event_seq_times_prev_credit_limit"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_clean"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.event_seq",
        F.col("c.account_type"),
        F.col("c.balance"),
    )
)

matched_keys = (
    with_latest
    .select("id", "country")
    .subtract(
        df_customers
        .filter(F.col("city").isin(["Bangkok", "Singapore"]))
        .select("id", "country")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

with_windows = kept_rows.withColumn("country_min_balance", F.min("balance").over(w3))

result = (
    with_windows
    .select(
        "id",
        "country",
        "balance",
        "country_min_balance",
        F.when(F.col("country").rlike("^(US|JP)$"), "CORE").when(F.col("country").rlike("^(TH|SG)$"), "GROUP_B").otherwise("EXTENDED").alias("country_zone"),
    )
    .orderBy(F.col("country_min_balance").desc(), "id")
)
