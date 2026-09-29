from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("employee_hire_date").desc_nulls_first(), "id")
w_hist = Window.partitionBy("level", "manager_id").orderBy("employee_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id", "dept_id")
w4 = Window.partitionBy("manager_id").orderBy(F.col("employee_salary").asc_nulls_first())
w5 = Window.orderBy(F.col("employee_salary").desc_nulls_last())
w6 = Window.partitionBy("manager_id").orderBy("employee_salary", "id")
w7 = Window.partitionBy("dept_id").orderBy(F.col("next_employee_salary").desc())
w8 = Window.partitionBy("dept_id").orderBy("employee_salary_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("dept_id").orderBy(F.col("employee_salary_cume_dist").desc_nulls_last())
w10 = Window.partitionBy("dept_id").orderBy(F.col("employee_salary").asc_nulls_first())

joined = (
    df_employees
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.level") != "L5")
    .select(
        "a.id",
        "a.email",
        "a.dept_id",
        "a.manager_id",
        "a.level",
        F.col("p.salary").alias("employee_salary"),
        F.col("p.hire_date").alias("employee_hire_date"),
    )
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "dept_id",
        "employee_salary",
        "employee_hire_date",
        "rank_in_group",
        F.sum("employee_salary").over(w_hist).alias("cumulative_employee_salary"),
        F.round((F.col("employee_salary") / F.sum("employee_salary").over(w3)), 4).alias("employee_salary_share"),
        F.cume_dist().over(w4).alias("employee_salary_cume_dist"),
        F.dense_rank().over(w5).alias("employee_salary_dense_rank"),
        F.lead("employee_salary", 1, 0).over(w6).alias("next_employee_salary"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.rank().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "dept_id",
        "employee_salary",
        "employee_hire_date",
        "rank_in_group",
        "cumulative_employee_salary",
        "employee_salary_share",
        "employee_salary_cume_dist",
        F.first("employee_salary_cume_dist", ignorenulls=True).over(w8).alias("first_known_employee_salary_cume_dist"),
        F.dense_rank().over(w9).alias("employee_salary_cume_dist_dense_rank"),
        F.cume_dist().over(w10).alias("employee_salary_cume_dist_2"),
    )
)
