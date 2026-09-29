from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", "id")
    .intersect(
        df_departments
        .filter(F.col("dept_name").isin(["Marketing", "Legal", "Sales"]))
        .select("dept_name", "id")
    )
)
