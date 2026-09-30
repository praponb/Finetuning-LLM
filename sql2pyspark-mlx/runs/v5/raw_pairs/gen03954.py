from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

joined = (
    flattened
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.city"), F.col("b.name"), F.col("b.age"))
)

base = (
    joined
    .filter(F.col("city").isNotNull() & (F.col("created_at") >= F.lit("2025-03-01 12:00:00").cast("timestamp")))
    .select(
        "id",
        "customer_id",
        "created_at",
        "city",
        "age",
        F.floor("age").alias("age_whole"),
        F.coalesce("code", F.lit("N/A")).alias("resolved_code"),
    )
)

enriched = (
    base
    .select(
        "customer_id",
        "created_at",
        "city",
        "age",
        "age_whole",
        "resolved_code",
        F.avg("age_whole").over(w).alias("age_whole_avg_2h"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("age_whole") >= 73, "LARGE").otherwise("SMALL").alias("age_whole_tier"),
        F.when(F.col("age_whole_avg_2h") >= 86, "HIGH").otherwise("LOW").alias("age_whole_avg_2h_tier"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("age_whole_avg_2h")).otherwise(0)).alias("chiang_mai_age_whole_avg_2h"),
        F.percentile_approx("age_whole", [0.25, 0.5, 0.75]).alias("age_whole_quartiles"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)
