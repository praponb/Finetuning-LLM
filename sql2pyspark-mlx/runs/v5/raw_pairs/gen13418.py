from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.year("hire_date") == 2025)
    .select("hire_date")
    .union(
        df_shipments
        .filter(F.col("shipped_date").between("2023-08-01", "2024-02-01"))
        .select("delivered_date")
    )
    .distinct()
    .orderBy(F.col("hire_date").desc())
)
