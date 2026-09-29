from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(~F.col("d.location").isin(["New York", "Berlin", "Bangkok"]) | (F.col("d.location") == "Chiang Mai"))
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("e.email"),
        F.when(F.col("d.location") == "Singapore", 1).otherwise(0).alias("is_singapore"),
    )
)
