from pyspark.sql import functions as F

result = (
    df_employees
    .select("id")
    .exceptAll(
        df_employees
        .filter(F.col("dept_id").isin([14, 16]))
        .select("id")
    )
)
