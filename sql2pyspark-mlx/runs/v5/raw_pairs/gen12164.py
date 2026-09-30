from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering"]))
    .select("location")
    .unionByName(
        df_departments
        .filter(F.col("budget").between(3532000, 3729000))
        .select("location")
    )
    .distinct()
    .orderBy("location")
)
