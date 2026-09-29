from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc())
w2 = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").desc())
w3 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country")
w_ordered = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy(F.col("age_percent_rank").desc(), "id")
w7 = Window.orderBy(F.col("first_known_credit_limit").desc(), "id")

base = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .withColumn("city_prefix", F.substring("city", 1, 4))
)

scored = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("age_percent_rank"),
        F.rank().over(w2).alias("credit_limit_rank"),
        F.first("credit_limit", ignorenulls=True).over(w3).alias("first_known_credit_limit"),
        F.round((F.col("age") / F.sum("age").over(w4)), 4).alias("age_share"),
        F.first("city", ignorenulls=True).over(w_ordered).alias("first_known_city"),
    )
)

filtered = (
    scored
    .filter((F.col("signup_date") > F.lit("2024-04-12").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "signup_date",
        "age",
        "age_percent_rank",
        "first_known_credit_limit",
        "first_known_city",
        F.when(F.col("country").isin(["SG", "TH"]), "Group A").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

scored_customers = (
    filtered
    .withColumn("age_percent_rank_bucket", F.ntile(4).over(w6))
    .withColumn("first_known_credit_limit_bucket", F.ntile(5).over(w7))
)

result = (
    scored_customers
    .cube("country", F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("country") == "SG", F.col("age")).otherwise(0)).alias("sg_age"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("age_per_jp"),
        F.bit_or("age").alias("age_bit_or"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("country") == "DE", F.col("first_known_credit_limit"))).alias("de_total_first_known_credit_limit"),
        F.max("first_known_credit_limit").alias("max_first_known_credit_limit"),
    )
    .filter(F.col("age_bit_or") > 1)
    .orderBy(F.col("sg_age").desc())
)
