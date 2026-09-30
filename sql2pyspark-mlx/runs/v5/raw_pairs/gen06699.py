from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Legal", "Finance"]))
    .select("id", "location")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isNotNull())
        .select("id", "location")
    )
)
