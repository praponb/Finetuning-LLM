from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment")
    .distinct()
    .crossJoin(df_employees.select("level").distinct())
    .orderBy("segment", "level")
)
