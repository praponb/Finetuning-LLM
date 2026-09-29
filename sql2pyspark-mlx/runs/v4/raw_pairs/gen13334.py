from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("age", "id")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")
w6 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

scored = (
    df_customers
    .select(
        "*",
        F.last("city", ignorenulls=True).over(w).alias("last_known_city"),
        F.lag("age", 2).over(w2).alias("prev_age"),
    )
)

scored_customers = (
    scored
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w3))
    .withColumn("city_avg_prev_age", F.avg("prev_age").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w6))
)

cleaned = (
    scored_customers
    .filter(F.col("signup_date") >= F.lit("2023-06-29").cast("date"))
    .select(
        "*",
        F.round((F.col("event_seq") / F.col("city_avg_prev_age")), 2).alias("event_seq_per_city_avg_prev_age"),
        F.round((F.col("event_seq") / F.col("age")), 2).alias("event_seq_per_age"),
    )
)

result = (
    cleaned
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "event_seq_tier"], ["quarter_start"], []],
        "quarter_start",
        "event_seq_tier",
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
)
