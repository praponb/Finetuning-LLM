from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("age_num")
w_part_2 = Window.partitionBy("segment").orderBy("signup_date", "id")
w3 = Window.partitionBy("segment")
w4 = Window.partitionBy("segment", "country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country")
w_recent = Window.orderBy("age_num_dense_rank", "id")
w_running = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("segment").orderBy("age_num_dense_rank", "id")
w9 = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit").desc(), "id")
w10 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-08-17"))) & (F.col("segment") == "Corporate"))
    .withColumn("age_num", F.col("age").cast("double"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

scored = (
    filtered
    .withColumn("age_num_dense_rank", F.dense_rank().over(w_part))
    .withColumn("next_age_num", F.lead("age_num", 1).over(w_part_2))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w3)), 4))
    .withColumn("running_age_num", F.sum("age_num").over(w4))
)

scored_customers = (
    scored
    .withColumn("country_stddev_samp_age_num_dense_rank", F.stddev_samp("age_num_dense_rank").over(w5))
    .withColumn("age_num_dense_rank_bucket", F.ntile(5).over(w_recent))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w_running))
)

enriched = (
    scored_customers
    .withColumn("prev_age_num_dense_rank", F.lag("age_num_dense_rank", 2, 0).over(w8))
    .withColumn("country_stddev_samp_age_num_dense_rank_share", F.round((F.col("country_stddev_samp_age_num_dense_rank") / F.sum("country_stddev_samp_age_num_dense_rank").over(w3)), 4))
    .withColumn("event_seq", F.row_number().over(w9))
    .withColumn("first_known_age_num_dense_rank", F.first("age_num_dense_rank", ignorenulls=True).over(w10))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("credit_limit") >= 12500, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.round(F.sum("age_num_dense_rank_bucket") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("age_num_dense_rank_bucket_per_smb"),
        F.sum(F.when(F.col("country") == "TH", F.col("age_num_dense_rank")).otherwise(0)).alias("th_age_num_dense_rank"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("th_age_num_dense_rank") > 0)
    .orderBy("credit_limit_tier")
)
