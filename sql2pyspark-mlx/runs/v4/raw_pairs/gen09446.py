from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id")
w2 = Window.partitionBy("segment").orderBy("age", "id")
w3 = Window.partitionBy("segment")
w4 = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_first())
w5 = Window.partitionBy("city").orderBy("age", "id")

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-10-30").cast("date")) & ~F.col("is_active"))
    .select(
        "*",
        F.initcap(F.trim("city")).alias("city_title"),
        F.initcap(F.trim("email")).alias("email_title"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_clean"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date") >= F.col("s.signup_date")), "left_anti")
)

enriched = (
    not_flagged
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("next_age", F.lead("age", 1).over(w2))
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w3))
    .withColumn("age_dense_rank", F.dense_rank().over(w4))
    .withColumn("event_seq_2", F.row_number().over(w5))
)

result = (
    enriched
    .select(
        "id",
        "country",
        F.col("corr_credit_limit_age").alias("CORR_CREDIT_LIMIT_AGE_VAL"),
        "credit_limit",
    )
    .unpivot(
        ["id", "country"],
        ["CORR_CREDIT_LIMIT_AGE_VAL", "credit_limit"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
