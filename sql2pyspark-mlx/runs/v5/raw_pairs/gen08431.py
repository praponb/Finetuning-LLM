from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("job_title").orderBy("salary")
w3 = Window.orderBy(F.col("salary").desc())
w4 = Window.orderBy("salary")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("level").eqNullSafe(F.lit("L5"))))
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
    )
)

remaining_keys = (
    valid
    .select("id", "job_title")
    .intersect(
        df_employees
        .filter(F.col("manager_id") == 32)
        .select("id", "job_title")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("level_max_salary", F.max("salary").over(w))
    .withColumn("salary_rank", F.rank().over(w2))
    .withColumn("salary_dense_rank", F.dense_rank().over(w3))
    .withColumn("salary_percent_rank", F.percent_rank().over(w4))
)

enriched_employees = (
    enriched
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.email"))
)

joined = (
    enriched_employees
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .filter(F.col("e.is_remote"))
    .select(
        "src.*",
        F.col("e.level").alias("employee_level"),
        F.col("e.salary").alias("employee_salary"),
    )
)

result = (
    joined
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "full_name",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        "email_user",
        "salary_rank",
        "salary_dense_rank",
        "salary_percent_rank",
        F.when(F.col("full_name").rlike("^(  Carol Diaz|Emma Brown)$"), "OTHER_ZONE").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "GROUP_B").otherwise("CORE").alias("full_name_zone"),
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("manager_id").desc_nulls_last())
)
