from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("salary_tier", F.when(F.col("salary") >= 164000, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("month", "hire_date"))
    .groupingSets(
        [["salary_tier", "activity_month"], ["salary_tier"], []],
        "salary_tier",
        "activity_month",
    )
    .agg(
        F.stddev_samp("salary").alias("salary_stddev"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_rows"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Intern", 1)), F.lit(0)), 2).alias("salary_per_intern"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("salary_per_intern").desc_nulls_last())
)
