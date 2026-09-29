from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level", "manager_id")
w3 = Window.partitionBy("level").orderBy(F.col("salary").desc())
w4 = Window.orderBy(F.col("salary_scaled").desc_nulls_first())
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

valid = (
    df_employees
    .filter(F.col("salary") > 84000)
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "level",
        F.round((F.col("salary") * 1.15), 2).alias("salary_scaled"),
    )
)

matched_keys = (
    valid
    .select("manager_id", "id")
    .subtract(
        df_employees
        .filter(F.col("salary") < 123000)
        .select("manager_id", "id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "id",
        "dept_id",
        "level",
        "salary_scaled",
        F.min("salary_scaled").over(w).alias("level_min_salary_scaled"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.rank().over(w3).alias("salary_rank"),
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share_2"),
        F.cume_dist().over(w4).alias("salary_scaled_cume_dist"),
    )
)

valid_employees = (
    with_windows
    .filter(F.col("level").isNotNull())
    .withColumn("greatest_salary_rank_salary_share_2", F.greatest("salary_rank", "salary_share_2"))
    .withColumn("salary_scaled_level", F.when(F.col("salary_scaled") >= 84, "High").when(F.col("salary_scaled") >= 28, "Medium").otherwise("Low"))
)

result = (
    valid_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "level",
        "salary_scaled",
        "salary_share",
        "salary_rank",
        "salary_share_2",
        "salary_scaled_cume_dist",
        "salary_scaled_level",
        F.when(F.col("level").rlike("^(L1|L4)$"), "GROUP_B").when(F.col("level").rlike("^(L2|L3)$"), "EXTENDED").otherwise("CORE").alias("level_zone"),
        F.col("min_employee_salary"),
    )
)
