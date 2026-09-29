from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_id", "level").orderBy("is_l5", "id")
w2 = Window.partitionBy("level", "manager_id").orderBy("hire_date", "id")

base = (
    df_employees
    .filter(F.length("full_name") < 10)
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("level") == "L5", 1).otherwise(0).alias("is_l5"),
    )
)

result = (
    base
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "level",
        "is_l5",
        F.lead("is_l5").over(w_grp).alias("next_is_l5"),
        F.lead("salary").over(w_grp).alias("next_salary"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.lead("is_l5", 1).over(w2).alias("next_is_l5_2"),
    )
)
