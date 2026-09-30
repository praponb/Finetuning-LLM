from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy("city").orderBy("signup_date", "id")
w4 = Window.partitionBy("segment", "city").orderBy(F.col("credit_limit").asc_nulls_first())

with_windows = (
    df_customers
    .filter(F.col("credit_limit") > 11800)
    .select(
        "*",
        F.lead("age", 2).over(w_part).alias("next_age"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .withColumn("prev_credit_limit", F.lag("credit_limit").over(w3))
    .withColumn("next_credit_limit", F.lead("credit_limit").over(w3))
    .withColumn("prev_next_age", F.lag("next_age").over(w3))
    .withColumn("next_next_age", F.lead("next_age").over(w3))
    .withColumn("event_seq_2", F.row_number().over(w3))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date")), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
    )
    .orderBy(F.col("segment").desc())
)
