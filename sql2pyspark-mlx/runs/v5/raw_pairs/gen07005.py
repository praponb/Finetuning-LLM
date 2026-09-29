from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "HR"]))
    .select("location")
    .unionByName(
        df_departments
        .filter(F.col("location") == "Chiang Mai")
        .select("location")
    )
    .orderBy("location")
)
