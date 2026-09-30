from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("salary").between(97000, 101000))
    .withColumn("full_name_slug", F.regexp_replace("full_name", " ", "-"))
)

agg = (
    valid_employees
    .groupBy(
        "job_title",
        F.when(F.col("level").isin(["L2", "L3"]), "tier_1").otherwise("other").alias("level_group"),
    )
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = agg.orderBy("job_title")
