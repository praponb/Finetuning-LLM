from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("city", "segment").orderBy(F.col("age").desc(), "id")
w3 = Window.orderBy("credit_limit", "id")
w4 = Window.partitionBy("city")
w5 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment")
w_running = Window.orderBy("corr_age_credit_limit", "id")
w8 = Window.partitionBy("segment", "city").orderBy("age", "id")

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-03-28"))) & F.col("segment").isNotNull() & (F.col("age") >= 31))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.trim("city").alias("clean_city"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.browser") == "Safari"), "left_semi")
)

enriched = (
    not_flagged
    .withColumn("credit_limit_bucket", F.ntile(4).over(w3))
    .withColumn("city_max_age", F.max("age").over(w4))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w5))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w4))
)

scored = (
    enriched
    .withColumn("segment_sum_age", F.sum("age").over(w6))
    .withColumn("next_city_max_age", F.lead("city_max_age", 1).over(w_running))
    .withColumn("age_bucket", F.ntile(5).over(w8))
)

result = (
    scored
    .groupBy("segment")
    .pivot("city", ["Tokyo", "Chiang Mai", "New York"])
    .agg(F.avg("age_bucket").alias("value_sum"))
    .select(
        "segment",
        F.col("Tokyo").alias("tokyo"),
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("New York").alias("new_york"),
    )
)
