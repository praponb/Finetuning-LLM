from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") < 75) & F.col("delivered_date").between("2023-03-01", "2023-10-01"))
    .select(
        "id",
        "carrier",
        "cost",
        "status",
        F.concat("status", F.lit(", "), "carrier").alias("status_carrier"),
    )
)
