from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("manager_id", "job_title")
    .intersectAll(
        df_employees
        .filter(F.col("salary") <= 67000)
        .select("manager_id", "job_title")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("level").rlike("^(L3|L4)$"), "OTHER_ZONE").when(F.col("level").rlike("^(L1|L2)$"), "EXTENDED").otherwise("GROUP_A").alias("level_zone"),
    )
    .orderBy(F.col("manager_id").desc_nulls_last())
)
