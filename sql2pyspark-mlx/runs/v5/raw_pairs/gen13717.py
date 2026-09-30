from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") != "Engineer"), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.substring("dept_name", 1, 4).alias("dept_name_prefix"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date").between("2024-07-01", "2025-05-01")), "left_semi")
)

valid = (
    not_flagged
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "location",
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "location",
        "location_filled",
        "location_filled_2",
        F.when(F.col("location").rlike("^(New York|London)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("location_zone"),
    )
    .orderBy(F.col("location").desc_nulls_first())
)
