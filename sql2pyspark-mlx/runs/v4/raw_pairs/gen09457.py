from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "level",
        F.when(F.col("salary") >= 147000, "High").when(F.col("salary") >= 42000, "Medium").otherwise("Low").alias("salary_level"),
    )
)

cleaned = (
    base
    .filter((F.col("hire_date") >= F.lit("2024-03-27").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "hire_date",
        "level",
        "salary_level",
        F.concat("level", F.lit(", "), "email").alias("level_email"),
        F.substring("job_title", 1, 2).alias("job_title_prefix"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

base_employees = (
    cleaned
    .filter((F.col("hire_date") >= F.lit("2023-07-06").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "hire_date",
        "level",
        "level_email",
        "job_title_prefix",
        "job_title_clean",
        F.regexp_replace("email", " ", "").alias("email_slug"),
        F.when(F.col("level").isin(["L1", "L3"]), "Tier 1").otherwise("Other").alias("level_group"),
    )
)

result = (
    base_employees
    .select(
        "id",
        "full_name",
        "email",
        "hire_date",
        "level_email",
        "job_title_clean",
        "email_slug",
        F.when(F.col("full_name").rlike("^(Jack Taylor|Noah Park)$"), "GROUP_A").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "EXTENDED").otherwise("CORE").alias("full_name_zone"),
    )
    .orderBy(F.col("hire_date").desc_nulls_first())
)
