from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("email")
    .intersect(
        df_employees
        .filter(F.month("hire_date").isin([5, 11, 12]))
        .select("email")
    )
)
