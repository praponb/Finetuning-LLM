from pyspark.sql import functions as F

result = (
    df_departments
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("location") != "Berlin")
        .select("id")
    )
)
