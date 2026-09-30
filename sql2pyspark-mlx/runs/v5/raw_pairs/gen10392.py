from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.orderBy(F.col("salary").desc())
w3 = Window.partitionBy("manager_id", "dept_id").orderBy(F.col("salary").desc())
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("dept_id").orderBy("salary")

kept_keys = (
    df_employees
    .select("manager_id", "id")
    .subtract(
        df_employees
        .filter(~F.col("is_remote"))
        .select("manager_id", "id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.lower(F.trim(F.col("d.email"))) == "ivy@gmail.com")
    .select(
        "a.id",
        "a.email",
        "a.dept_id",
        "a.manager_id",
        "a.salary",
        "a.hire_date",
        "a.is_remote",
        "a.level",
        F.col("d.email").alias("employee_email"),
    )
)

enriched = (
    with_dims
    .withColumn("dept_id_max_salary", F.max("salary").over(w))
    .withColumn("salary_dense_rank", F.dense_rank().over(w2))
    .withColumn("salary_percent_rank", F.percent_rank().over(w3))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w4)), 4))
    .withColumn("salary_dense_rank_2", F.dense_rank().over(w5))
)

remaining_keys = (
    enriched
    .select("level", "manager_id")
    .intersectAll(
        df_employees
        .filter(F.year("hire_date") >= 2023)
        .select("level", "manager_id")
    )
)

reconciled_employees = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    reconciled_employees
    .select("dept_id", "id")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([3, 23, 31, 37]))
        .select("dept_id", "id")
    )
)

verified = (
    reconciled_employees
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "employee_email",
        "dept_id_max_salary",
        "salary_dense_rank",
        "salary_percent_rank",
    )
    .orderBy("salary_percent_rank", "salary_dense_rank")
)
