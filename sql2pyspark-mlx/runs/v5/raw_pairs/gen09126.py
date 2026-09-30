from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3644000)
    .select("id", "location")
    .intersect(
        df_departments
        .filter(F.col("location") == "London")
        .select("id", "location")
    )
)
