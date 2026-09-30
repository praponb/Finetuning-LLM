from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Emm%") & (F.col("age") < 38))
    .select("id", "city", "name", "signup_date")
    .orderBy("signup_date", "city")
)
