from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("city").orderBy("age", "id")
w3 = Window.partitionBy("country").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy(F.col("next_age").asc_nulls_first(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

latest = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "email",
        "city",
        "segment",
        "credit_limit",
        "age",
        F.lag("credit_limit").over(w2).alias("prev_credit_limit"),
        F.lead("credit_limit").over(w2).alias("next_credit_limit"),
        F.lag("age").over(w2).alias("prev_age"),
        F.lead("age").over(w2).alias("next_age"),
        F.row_number().over(w2).alias("row_seq"),
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.end_date"))
)

latest_match_customers = (
    latest_match
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.end_date").alias("subscription_end_date"))
)

result = (
    latest_match_customers
    .select(
        "id",
        "city",
        "segment",
        "credit_limit",
        "age",
        "prev_credit_limit",
        "prev_age",
        "next_age",
        "dedup_rank",
        F.when(F.col("city").rlike("^(Berlin|Singapore)$"), "EXTENDED").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy("id")
)
