from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.month("hire_date").alias("hire_month"),
        F.when(F.col("job_title").isin(["Director", "Manager"]), "tier_1").otherwise("other").alias("job_title_group"),
    )
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.countDistinct("salary").alias("unique_levels"),
    )
    .filter(F.col("unique_levels") > 1)
)
