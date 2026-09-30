from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Finance"]))
    .select("dept_name")
    .unionByName(
        df_departments
        .filter(F.col("budget").between(2862000, 3736000))
        .select("dept_name")
    )
    .orderBy("dept_name")
)
