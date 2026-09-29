from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.orderBy(F.col("age").desc_nulls_first())
w5 = Window.partitionBy("segment").orderBy("cumulative_age", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("country").orderBy(F.col("days_since_signup_date").asc_nulls_first())

base = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-09-30").cast("date")) & F.col("segment").isNotNull() & ~F.col("is_active"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.lower("segment").alias("segment_lower"),
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.sum("age").over(w2).alias("cumulative_age"),
        F.round((F.col("age") / F.sum("age").over(w3)), 4).alias("age_share"),
        F.cume_dist().over(w4).alias("age_cume_dist"),
    )
)

scored = (
    enriched
    .withColumn("age_cume_dist_avg_3", F.avg("age_cume_dist").over(w5))
    .withColumn("days_since_signup_date_rank", F.rank().over(w6))
)

result = (
    scored
    .select(
        "id",
        "city",
        F.col("age_share").alias("AGE_SHARE"),
        F.col("age_cume_dist").alias("AGE_CUME_DIST_VAL"),
    )
    .unpivot(["id", "city"], ["AGE_SHARE", "AGE_CUME_DIST_VAL"], "measure", "reading")
    .filter(F.col("reading").isNotNull())
)
