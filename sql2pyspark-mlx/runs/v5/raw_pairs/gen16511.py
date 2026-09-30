from pyspark.sql import functions as F

result = (
    df_stores
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.col("opened_date").between("2023-05-01", "2023-08-01"))
        .select("region", "city")
    )
)
