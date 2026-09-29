from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget").between(1251000, 3412000) & (F.col("budget") < 3634000)) | F.col("location").isin(["New York", "Berlin"]))
    .select("budget", "location", F.round("budget", 2).alias("budget_rounded"))
)
