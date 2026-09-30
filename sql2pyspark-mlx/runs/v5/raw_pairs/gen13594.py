from pyspark.sql import functions as F

min_budget = df_departments.filter(F.col("budget") > 3730000).agg(F.min("budget")).first()[0]

result = (
    df_departments
    .filter(F.col("budget") == min_budget)
    .select("id", "budget")
)
