from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-05-11").cast("date"))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "is_remote",
        "level",
        F.regexp_replace("email", " ", "-").alias("email_slug"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.level").alias("employee_level"), F.col("d.full_name"))
)

cleaned = (
    with_dims
    .filter(F.col("manager_id").isNotNull() & (F.length("full_name") >= 11))
    .select(
        "id",
        "email",
        "dept_id",
        "level",
        "level_filled",
        "employee_level",
        "full_name",
        F.regexp_replace("full_name", " ", "_").alias("full_name_slug"),
    )
)

result = (
    cleaned
    .rollup("employee_level")
    .agg(
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.count_if(F.col("employee_level") == "L4").alias("l4_events"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("employee_level")).alias("num_employee_levels"),
        F.grouping("employee_level").alias("gid"),
    )
)
