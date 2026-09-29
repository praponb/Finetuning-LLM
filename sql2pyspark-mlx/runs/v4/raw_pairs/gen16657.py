from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", "id")
    .intersect(
        df_departments
        .filter(F.col("budget") <= 1320000)
        .select("location", "id")
    )
)
