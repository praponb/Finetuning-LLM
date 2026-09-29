from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "country")
    .intersectAll(
        df_customers
        .filter(F.col("age") != 31)
        .select("id", "country")
    )
)
