from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", "id")
    .subtract(
        df_departments
        .filter(F.col("location").isin(["New York", "London", "Bangkok"]))
        .select("location", "id")
    )
)
