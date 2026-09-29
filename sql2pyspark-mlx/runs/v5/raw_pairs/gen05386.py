from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country").orderBy("credit_limit", "id")
w3 = Window.partitionBy("segment", "country").orderBy("is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment", "city").orderBy(F.col("signup_date").desc(), "id")
w5 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w6 = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")

valid = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-06-28").cast("date"))
    .select("*", F.col("is_active").cast("int").alias("is_active_flag"))
)

enriched = (
    valid
    .filter(F.upper(F.trim("name")) == "CAROL DIAZ")
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.avg("age").over(w).alias("city_avg_age"),
        F.ntile(5).over(w2).alias("credit_limit_bucket"),
        F.last("segment", ignorenulls=True).over(w3).alias("last_known_segment"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .orderBy(F.col("activity_month").desc())
)
