from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("credit_limit", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Corporate"]))
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.when(F.col("country").isin(["UK", "JP"]), "Priority").otherwise("Other").alias("country_group"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

metrics = (
    latest_match
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "email",
        "segment",
        "credit_limit",
        "age",
        F.min("age").over(w2).alias("last3_min_age"),
        F.count("age").over(w2).alias("last3_count_age"),
        F.max("age").over(w2).alias("last3_max_age"),
        F.lead("age", 1).over(w3).alias("next_age"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isin([4, 17, 18])), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.segment",
        "s.age",
        "s.last3_min_age",
        "s.last3_count_age",
        "s.last3_max_age",
        "s.next_age",
        F.col("c.status").alias("subscription_status"),
        F.col("c.plan"),
    )
)

result = (
    with_last
    .select(
        "id",
        "segment",
        "age",
        "last3_min_age",
        "last3_count_age",
        "last3_max_age",
        "next_age",
        "subscription_status",
    )
    .orderBy("age")
)
