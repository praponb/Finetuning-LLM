from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-05-28"))) & F.col("job_title").isNotNull())
    .withColumn("full_name_email", F.concat("full_name", F.lit(" - "), "email"))
)

result = (
    filtered
    .groupBy(
        "manager_id",
        F.when(F.col("salary") >= 161000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L1", 1)), F.lit(0)), 2).alias("salary_per_l1"),
    )
    .filter(F.col("salary_per_l1") > 0)
    .orderBy(F.col("salary_per_l1").asc_nulls_last())
)
