from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("job_title") != "Designer")
    .withColumn("full_name_normalized", F.upper(F.trim("full_name")))
)

agg = (
    base
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = agg.select("hire_cohort", "level_values", "level_list")
