from pyspark.sql import functions as F

result = (
    df_departments
    .select("location")
    .exceptAll(
        df_departments
        .filter(F.col("budget") <= 2916000)
        .select("location")
    )
)
