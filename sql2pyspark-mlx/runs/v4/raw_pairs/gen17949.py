from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "*",
        F.round((F.col("salary") * 1.15), 2).alias("salary_scaled"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("month", "hire_date").alias("activity_month"))
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Director", 1)), F.lit(0)), 2).alias("salary_per_director"),
    )
    .orderBy(F.col("salary_per_director").desc_nulls_last())
)
