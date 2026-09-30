from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")
w2 = Window.partitionBy("segment").orderBy(F.col("credit_limit_filled").desc())

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date_2"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.segment"))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.start_date",
        "s.days_since_end_date",
        "s.days_since_end_date_2",
        "s.segment",
        F.col("p.credit_limit"),
        F.col("p.name"),
        F.col("p.country"),
    )
)

filtered = (
    joined
    .filter(F.col("start_date") >= F.lit("2023-05-27").cast("date"))
    .select(
        "*",
        F.concat("name", F.lit(", "), "segment").alias("name_segment"),
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

base_subscriptions = (
    deduped
    .filter(F.col("start_date") > F.lit("2023-03-11").cast("date"))
    .withColumn("least_days_since_end_date_2_days_since_end_date", F.least("days_since_end_date_2", "days_since_end_date"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("NONE")))
)

totals = (
    base_subscriptions
    .groupBy(
        F.when(F.col("days_since_end_date") >= 66, "LARGE").otherwise("SMALL").alias("days_since_end_date_tier"),
        "customer_id",
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.size(F.collect_set("segment")).alias("num_segments"),
    )
)

result = (
    totals
    .select("days_since_end_date_tier", "customer_id", "country", "segment_list")
    .orderBy("days_since_end_date_tier")
)
