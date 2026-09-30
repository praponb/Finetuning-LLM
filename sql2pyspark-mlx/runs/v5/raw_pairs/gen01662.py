from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

filtered = (
    exploded
    .filter(F.to_date("created_at") < "2025-05-29")
    .withColumn("created_at_period", F.date_trunc("year", "created_at"))
)

base = (
    filtered
    .filter(F.col("created_at") >= F.to_date(F.lit("2025-03-02")))
    .select(
        "customer_id",
        "created_at",
        "key",
        "value",
        "created_at_period",
        F.quarter("created_at").alias("created_at_quarter"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("c.country").isin(["SG", "UK", "TH"]))
    .select(
        "src.customer_id",
        "src.key",
        "src.created_at_period",
        "src.created_at_quarter",
        "src.resolved_key",
        F.col("c.segment"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.signup_date"), F.col("p.credit_limit"), F.col("p.age"))
)

result = (
    with_dims
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .rollup("key", "customer_id", "activity_month")
    .agg(
        F.avg(F.when(F.col("key") == "gift", F.col("created_at_quarter"))).alias("gift_avg_created_at_quarter"),
        F.max("created_at_quarter").alias("max_created_at_quarter"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("key").desc())
)
