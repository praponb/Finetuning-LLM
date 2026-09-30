from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("budget") > 1337000)
        .select("id", "location")
    )
)
