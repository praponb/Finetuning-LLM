from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "country").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("burst_id").desc())

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-06-03").cast("date"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.length("browser").alias("browser_length"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.email"), F.col("c.signup_date"), F.col("c.credit_limit"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.name"))
)

with_gaps = (
    with_dims
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "signup_date",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

ranked = (
    sessionized
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    ranked
    .select(
        "*",
        F.lead("burst_id", 1, 0).over(w4).alias("next_burst_id"),
        F.dense_rank().over(w5).alias("burst_id_dense_rank"),
    )
)

result = (
    scored
    .groupBy(
        "country",
        F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
    )
    .agg(
        F.avg("next_burst_id").alias("avg_next_burst_id"),
        F.percentile_approx("burst_id_dense_rank", 0.95).alias("burst_id_dense_rank_p95"),
        F.sum(F.when(F.col("country") == "US", F.col("burst_id")).otherwise(0)).alias("us_burst_id"),
        F.sum("next_burst_id").alias("total_next_burst_id"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)
