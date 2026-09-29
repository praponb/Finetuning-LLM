from pyspark.sql import functions as F

with_dims = (
    df_sessions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.credit_limit"), F.col("p.segment"), F.col("p.age"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("credit_limit") >= 17500, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
    )
    .agg(
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.sum(F.when(F.col("country") == "JP", F.col("credit_limit")).otherwise(0)).alias("jp_credit_limit"),
    )
    .orderBy(F.col("jp_credit_limit").asc_nulls_last())
)
