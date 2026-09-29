from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")
w2 = Window.partitionBy("country", "city").orderBy("rank_in_group", "id")
w3 = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_last())
w4 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("device").orderBy(F.col("age_cume_dist").desc_nulls_first(), "id")
w7 = Window.partitionBy("device")
w_part = Window.partitionBy("city").orderBy("age_cume_dist")
w9 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter(F.col("segment") == "Corporate")
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        F.trim("city").alias("clean_city"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_filled"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .withColumn("next_credit_limit", F.lead("credit_limit", 2).over(w2))
    .withColumn("age_cume_dist", F.cume_dist().over(w3))
    .withColumn("first_known_age", F.first("age", ignorenulls=True).over(w4))
)

with_last = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device"))
)

latest = (
    with_last
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .withColumn("first_known_age_share", F.round((F.col("first_known_age") / F.sum("first_known_age").over(w7)), 4))
    .withColumn("age_cume_dist_cume_dist", F.cume_dist().over(w_part))
    .withColumn("cumulative_next_credit_limit", F.sum("next_credit_limit").over(w9))
)

result = (
    metrics
    .select(
        "id",
        "device",
        F.col("age_cume_dist_cume_dist").alias("AGE_CUME_DIST_CUME_DIST_VAL"),
        F.col("next_credit_limit").alias("NEXT_CREDIT_LIMIT"),
        F.col("age_cume_dist").alias("AGE_CUME_DIST"),
    )
    .unpivot(
        ["id", "device"],
        ["AGE_CUME_DIST_CUME_DIST_VAL", "NEXT_CREDIT_LIMIT", "AGE_CUME_DIST"],
        "measure",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
