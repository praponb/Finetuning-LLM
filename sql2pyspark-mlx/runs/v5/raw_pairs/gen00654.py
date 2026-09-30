from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee_whole", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_avg_6_prior").desc_nulls_last(), "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.ceil("monthly_fee").alias("monthly_fee_whole"))
)

metrics = (
    valid
    .filter(F.col("status") == "CANCELLED")
    .withColumn("monthly_fee_avg_6_prior", F.avg("monthly_fee").over(w))
    .withColumn("monthly_fee_rank", F.rank().over(w2))
    .withColumn("next_monthly_fee", F.lead("monthly_fee", 1).over(w3))
)

enriched = (
    metrics
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.end_date",
        "a.monthly_fee_whole",
        "a.monthly_fee_avg_6_prior",
        "a.next_monthly_fee",
        F.col("c.age"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

valid_subscriptions = (
    deduped
    .filter(F.col("monthly_fee_whole").between(60, 73))
    .withColumn("latest_rank_rounded", F.round("latest_rank", 2))
)

with_dims = (
    valid_subscriptions
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.city"))
)

result = (
    with_dims
    .select("id", "customer_id", "next_monthly_fee", "latest_rank_rounded")
    .unpivot(
        ids=["id", "customer_id"],
        values=["next_monthly_fee", "latest_rank_rounded"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
