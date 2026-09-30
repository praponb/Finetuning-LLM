from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("opened_date").between("2025-03-01", "2025-04-01"))
        .select("city")
    )
    .orderBy(F.col("city").desc())
)
