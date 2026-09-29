from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment", "city")
    .intersectAll(
        df_customers
        .filter(F.col("city").isNotNull())
        .select("segment", "city")
    )
)
