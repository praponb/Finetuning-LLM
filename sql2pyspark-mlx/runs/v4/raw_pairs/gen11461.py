from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-05-09", "2023-07-01"))
    .select("id", "delivered_date", "carrier")
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date").between("2024-01-04", "2024-07-01"))
        .select("id", "delivered_date", "carrier")
    )
)
