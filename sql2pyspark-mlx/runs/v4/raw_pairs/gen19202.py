from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id")
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("age").desc_nulls_first())
w_running = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")
w_grp_2 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("credit_limit").desc())
w8 = Window.partitionBy("city").orderBy("last_known_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("credit_limit", "id").rowsBetween(-2, Window.currentRow)
w11 = Window.partitionBy("country", "city").orderBy("row_num", "id")

latest = (
    df_customers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

filtered = (
    latest
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-10-16")))
    .select("*", F.regexp_replace("name", " ", "").alias("name_slug"))
)

with_windows = (
    filtered
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w2))
    .withColumn("age_dense_rank", F.dense_rank().over(w_part))
    .withColumn("age_avg_6", F.avg("age").over(w_running))
    .withColumn("credit_limit_bucket", F.ntile(4).over(w_grp))
    .withColumn("last_known_credit_limit", F.last("credit_limit", ignorenulls=True).over(w_grp_2))
)

scored = (
    with_windows
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w7))
    .withColumn("last_known_age_avg_6", F.last("age_avg_6", ignorenulls=True).over(w8))
    .withColumn("last_known_segment", F.last("segment", ignorenulls=True).over(w9))
    .withColumn("age_avg_2", F.avg("age").over(w10))
    .withColumn("next_credit_limit_bucket", F.lead("credit_limit_bucket", 2).over(w11))
)

filtered_customers = (
    scored
    .filter((F.col("signup_date") >= F.lit("2023-05-18").cast("date")) & F.col("city").isNotNull())
    .withColumn("age_avg_2_rounded", F.round("age_avg_2", 2))
)

result = (
    filtered_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
    )
    .orderBy(F.col("credit_limit").desc_nulls_first(), "id")
)
