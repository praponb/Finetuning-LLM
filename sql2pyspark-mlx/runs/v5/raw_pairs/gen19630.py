from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-09-23")))
    .withColumn("is_jp", F.when(F.col("country") == "JP", 1).otherwise(0))
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.segment",
        "s.signup_date",
        "s.age",
        "s.is_jp",
        "s.rank_in_group",
        F.col("c.status").alias("subscription_status"),
    )
)

latest_match_customers = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee").alias("order_shipping_fee"))
)

result = (
    latest_match_customers
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "is_jp",
        "rank_in_group",
        "subscription_status",
        F.when(F.col("subscription_status").rlike("^(ACTIVE|EXPIRED)$"), "CORE").when(F.col("subscription_status").rlike("^(CANCELLED|PAUSED)$"), "GROUP_A").otherwise("GROUP_B").alias("subscription_status_zone"),
    )
)
