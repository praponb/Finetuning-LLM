from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

cleaned = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-03-27")))
    .select(
        "*",
        F.add_months("signup_date", 3).alias("signup_date_plus_months"),
        F.when(F.col("credit_limit") >= 14500, "High").when(F.col("credit_limit") >= 5300, "Medium").otherwise("Low").alias("credit_limit_level"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") >= F.col("s.signup_date_plus_months")), "left_anti")
)

latest = (
    not_flagged
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

totals = (
    latest
    .cube(
        F.when(F.col("credit_limit") >= 13500, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)

result = (
    totals
    .select("chiang_mai_count", "city_list")
    .orderBy(F.col("chiang_mai_count").desc_nulls_last())
)
