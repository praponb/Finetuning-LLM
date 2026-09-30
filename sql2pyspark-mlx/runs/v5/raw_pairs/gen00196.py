from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("end_date").desc_nulls_first(), "id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-09-13").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

filtered_subscriptions = (
    filtered
    .filter((F.col("end_date") >= F.lit("2023-06-12").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status_prefix",
        F.when(F.col("plan").isin(["basic", "standard"]), "Core").otherwise("Other").alias("plan_group"),
    )
)

deduped = (
    filtered_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.name"), F.col("c.age"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.name").like("bob%"))
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.end_date",
        "a.status_prefix",
        F.col("c.credit_limit"),
        F.col("c.name").alias("customer_name"),
        F.col("c.signup_date"),
    )
)

result = (
    with_dims
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.city").alias("city"), F.col("src.plan").alias("plan"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.credit_limit")).alias("min_credit_limit"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
