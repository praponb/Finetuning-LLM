from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@company.co"))
    .select("email")
    .unionByName(
        df_employees
        .filter(F.col("manager_id").isin([2, 19, 23, 38]))
        .select("email")
    )
    .distinct()
)
