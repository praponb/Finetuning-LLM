from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("manager_id")
    .subtract(
        df_employees
        .filter(F.col("level") == "L2")
        .select("manager_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter((F.col("hire_date") > F.lit("2023-02-26").cast("date")) & (F.year("hire_date") == 2025))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.coalesce("full_name", F.lit("unassigned")).alias("full_name_filled"),
        F.coalesce("level", F.lit("unassigned")).alias("level_filled"),
        F.coalesce("level", F.lit("N/A")).alias("level_clean"),
    )
)

cleaned = (
    valid
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-07-08"))) & (F.col("hire_date") >= "2025-05-01"))
    .select(
        "id",
        "email",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        "full_name_filled",
        "level_clean",
        F.when(F.col("level").isin(["L1", "L3"]), "Priority").otherwise("Other").alias("level_group"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled_2"),
    )
)

valid_employees = (
    cleaned
    .filter((F.col("hire_date") >= F.lit("2023-09-12").cast("date")) & F.col("level").isNotNull() & F.col("manager_id").isin([4, 27]))
    .select("*", F.substring("email", 1, 2).alias("email_prefix"))
)

base = (
    valid_employees
    .filter(F.col("email").like("%@company.co"))
    .withColumn("level_filled", F.coalesce("level", F.lit("N/A")))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
)

remaining_keys = (
    base
    .select("id", "level")
    .subtract(
        df_employees
        .filter(F.year("hire_date") == 2024)
        .select("id", "level")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .select("src.*", F.col("e.email").alias("employee_email"))
)

result = (
    enriched
    .groupBy("manager_id")
    .pivot("level", ["L2", "L4"])
    .agg(F.max("is_remote_flag").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "manager_id",
        F.col("L2_amount").alias("l2_amount"),
        F.col("L2_n").alias("l2_n"),
        F.col("L4_amount").alias("l4_amount"),
        F.col("L4_n").alias("l4_n"),
    )
)
