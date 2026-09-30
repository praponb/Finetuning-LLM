from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(F.col("opened_date").between("2025-01-01", "2025-08-23") & F.col("city").isin(["Tokyo", "Berlin"])))
    .orderBy("region", "city")
)
