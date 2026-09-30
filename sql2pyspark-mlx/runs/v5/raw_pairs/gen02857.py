from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .select("id", "shipped_date")
    .subtract(
        df_shipments
        .filter(F.col("carrier").isNull())
        .select("id", "shipped_date")
    )
)
