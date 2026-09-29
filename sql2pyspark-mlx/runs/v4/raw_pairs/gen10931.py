from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "is_remote",
        F.when(F.col("salary") >= 130000, "high").when(F.col("salary") >= 45000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .orderBy("salary_bucket", F.col("unique_managers").desc())
)
