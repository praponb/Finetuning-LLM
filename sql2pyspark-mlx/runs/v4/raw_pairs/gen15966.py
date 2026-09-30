from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 15463, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .groupingSets(
        [["sqft_tier", "activity_month"], ["sqft_tier"], []],
        "sqft_tier",
        "activity_month",
    )
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.grouping("sqft_tier").alias("grouping_id"),
    )
    .orderBy(F.col("sqft_bit_or").desc_nulls_first())
)
