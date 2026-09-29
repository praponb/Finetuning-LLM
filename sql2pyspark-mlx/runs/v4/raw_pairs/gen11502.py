from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-07-01", "2023-09-10"))
    .select("signup_date")
    .union(df_shipments.select("delivered_date"))
    .distinct()
    .orderBy("signup_date")
)
