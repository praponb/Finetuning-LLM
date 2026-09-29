from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-07-20").cast("date"))
    .withColumn("signup_date_label", F.date_format("signup_date", "yyyy-MM-dd"))
)

result = (
    base
    .groupBy("country")
    .agg(
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.bit_or("age").alias("age_bit_or"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.bit_and("age").alias("age_bit_and"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .filter(F.col("age_bit_or") >= 1)
    .orderBy(F.col("country").desc_nulls_first())
)
