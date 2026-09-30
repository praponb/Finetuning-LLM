from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .distinct()
    .crossJoin(df_sessions.select("browser").distinct())
    .orderBy("country", "browser")
)
