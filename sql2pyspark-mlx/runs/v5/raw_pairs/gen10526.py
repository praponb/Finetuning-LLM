from pyspark.sql import functions as F

result = (
    df_employees
    .select("dept_id", "level")
    .intersect(
        df_employees
        .filter(F.col("dept_id").isin([15, 25, 38]))
        .select("dept_id", "level")
    )
)
