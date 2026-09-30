from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB"]))
    .select("signup_date", "country")
    .subtract(
        df_customers
        .filter(F.length("name") > 10)
        .select("signup_date", "country")
    )
)
