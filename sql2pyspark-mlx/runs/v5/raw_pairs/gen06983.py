from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(F.col("full_name").isNull() & ~F.col("is_remote")))
    .select(
        "id",
        "manager_id",
        "level",
        "dept_id",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
    .orderBy("dept_id", "email_user")
)
