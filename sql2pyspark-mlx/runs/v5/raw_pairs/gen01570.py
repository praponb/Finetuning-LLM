from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("age_str", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w4 = Window.partitionBy("segment")
w_hist = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("running_min_age_share", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("age_str", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("city")
w9 = Window.partitionBy("country").orderBy("running_count_age", "id")

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-06-02").cast("date"))
    .select(
        "*",
        F.col("age").cast("double").alias("age_str"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.min("age").over(w_ordered).alias("running_min_age"),
        F.max("age").over(w_ordered).alias("running_max_age"),
        F.sum("age").over(w_ordered).alias("running_sum_age"),
        F.count("age").over(w_ordered).alias("running_count_age"),
        F.last("city", ignorenulls=True).over(w2).alias("last_known_city"),
    )
)

enriched_customers = (
    enriched
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("running_min_age_share", F.round((F.col("running_min_age") / F.sum("running_min_age").over(w4)), 4))
)

enriched_customers_2 = (
    enriched_customers
    .filter(F.col("running_min_age_share") > 0.34)
    .withColumn("first_known_running_max_age", F.first("running_max_age", ignorenulls=True).over(w_hist))
    .withColumn("running_min_age_share_avg_3", F.avg("running_min_age_share").over(w6))
    .withColumn("age_str_avg_5", F.avg("age_str").over(w7))
    .withColumn("city_sum_running_count_age", F.sum("running_count_age").over(w8))
    .withColumn("seq_no", F.row_number().over(w9))
)

summary = (
    enriched_customers_2
    .groupBy(F.date_trunc("WEEK", "signup_date").alias("week_start"))
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.bit_or("age").alias("age_bit_or"),
        F.max(F.when(F.col("city") == "London", F.col("seq_no"))).alias("london_max_seq_no"),
        F.percentile_approx("age_str_avg_5", [0.25, 0.5, 0.75]).alias("age_str_avg_5_quartiles"),
        F.count("*").alias("cnt"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
    )
)

result = summary.select("week_start", "sg_count", "age_bit_or", "age_str_avg_5_quartiles")
