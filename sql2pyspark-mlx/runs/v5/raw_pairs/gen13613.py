from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select("id", "name", "signup_date")
    .orderBy("name", "id")
    .limit(10)
)
