from pyspark.sql import functions as F

result = (
    df_departments
    .select("id")
    .intersect(
        df_departments
        .filter(F.col("budget").between(2789000, 3486000))
        .select("id")
    )
)
