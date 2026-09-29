from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") <= "2025-07-28") & (F.col("job_title") == "Manager"))
    .groupBy(
        F.when(F.col("level").isin(["L1", "L4"]), "group_a").otherwise("other").alias("level_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0))) / F.count("*")), 1).alias("pct_intern"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .filter(F.col("pct_intern") < 46)
    .orderBy(F.col("pct_intern").desc(), "level_group")
)
