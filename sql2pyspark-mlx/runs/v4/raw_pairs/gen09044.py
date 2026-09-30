from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("level", "manager_id").orderBy("is_remote_flag", "id")
w_recent = Window.partitionBy("manager_id").orderBy(F.col("is_remote_flag").desc(), "id")

valid = (
    df_employees
    .filter(F.col("level").isNotNull() & F.col("dept_id").isin([10, 16, 24, 32]))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

scored = (
    valid
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("next_is_remote_flag", F.lead("is_remote_flag", 2).over(w2))
    .withColumn("is_remote_flag_bucket", F.ntile(3).over(w_recent))
)

valid_employees = (
    scored
    .filter(F.col("hire_date") >= F.lit("2023-12-11").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "hire_date",
        "event_seq",
        "is_remote_flag_bucket",
        F.col("is_remote").cast("int").alias("is_remote_flag_2"),
    )
)

remaining_keys = (
    valid_employees
    .select("id")
    .subtract(
        df_employees
        .filter(F.col("email").isNotNull())
        .select("id")
    )
)

reconciled = (
    valid_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_employees_2 = (
    reconciled
    .filter(F.col("manager_id").isNotNull())
    .select(
        "*",
        F.when(F.col("is_remote_flag_2") >= 73, "High").when(F.col("is_remote_flag_2") >= 14, "Medium").otherwise("Low").alias("is_remote_flag_2_level"),
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)

with_dims = (
    valid_employees_2
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.salary"),
        F.col("b.hire_date").alias("employee_hire_date"),
    )
)

valid_employees_3 = (
    with_dims
    .filter(F.col("hire_date") >= F.lit("2023-06-12").cast("date"))
    .select(
        "id",
        "email",
        "manager_id",
        "hire_date",
        "is_remote_flag_bucket",
        "is_remote_flag_2",
        "is_remote_flag_2_level",
        "employee_full_name",
        "salary",
        F.round((F.col("is_remote_flag_2") * 1.15), 2).alias("is_remote_flag_2_scaled"),
    )
)

result = (
    valid_employees_3
    .select(
        "id",
        "email",
        "manager_id",
        "hire_date",
        "is_remote_flag_bucket",
        "is_remote_flag_2",
        "is_remote_flag_2_level",
        "employee_full_name",
        "salary",
        F.when(F.col("employee_full_name").rlike("^(  Carol Diaz|Emma Brown)$"), "PRIORITY").when(F.col("employee_full_name").rlike("^(Alice Wong|bob smith)$"), "CORE").otherwise("GROUP_A").alias("employee_full_name_zone"),
    )
    .orderBy(F.col("salary").asc_nulls_last())
)
