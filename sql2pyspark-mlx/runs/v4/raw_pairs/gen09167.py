from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3407000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
        F.countDistinct("location").alias("distinct_locations"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .filter(F.col("distinct_locations") >= 1)
)
