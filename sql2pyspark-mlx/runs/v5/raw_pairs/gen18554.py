from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4093000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.count_if(F.col("location") == "Chiang Mai").alias("chiang_mai_events"),
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "London", 1)), F.lit(0)), 2).alias("budget_per_london"),
    )
    .orderBy(F.col("sales_events").desc())
)
