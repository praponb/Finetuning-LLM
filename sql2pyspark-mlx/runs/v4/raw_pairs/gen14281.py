from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L4", "L5"]))
    .select("hire_date")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["Central", "West"]))
        .select("opened_date")
    )
    .orderBy("hire_date")
)
