from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("country") != "DE") & F.col("name").like("%Kanya%")) | F.col("signup_date").between("2024-05-30", "2024-08-24"))
    .select("credit_limit", "country")
    .orderBy("country")
)
