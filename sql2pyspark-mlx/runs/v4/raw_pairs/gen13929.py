from pyspark.sql import functions as F

enriched = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.segment"), F.col("d.city"), F.col("d.credit_limit"))
)

result = (
    enriched
    .groupBy("plan")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit_2"),
    )
    .orderBy(F.col("num_cities").desc())
)
