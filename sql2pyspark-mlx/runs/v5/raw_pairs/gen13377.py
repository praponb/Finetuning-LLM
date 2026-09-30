from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("monthly_fee").asc_nulls_last())
w3 = Window.partitionBy("customer_country", "plan")
w4 = Window.partitionBy("customer_id").orderBy(F.col("signup_date").desc_nulls_last(), "id")

enriched = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.segment").isin(["SMB", "Corporate", "Consumer"]))
    .select("e.*", F.col("p.country"), F.col("p.credit_limit"), F.col("p.signup_date"))
)

enriched_subscriptions = (
    enriched
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.*",
        F.col("b.email"),
        F.col("b.name"),
        F.col("b.country").alias("customer_country"),
    )
)

scored = (
    enriched_subscriptions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.percent_rank().over(w2).alias("monthly_fee_percent_rank"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4).alias("credit_limit_share"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "plan",
        "status",
        "credit_limit",
        "name",
        "customer_country",
        "monthly_fee_percent_rank",
    )
)

result = (
    deduped
    .select("id", "status", "name")
    .orderBy("id", F.col("status").desc())
)
