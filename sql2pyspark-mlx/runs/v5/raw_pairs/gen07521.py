from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Mi%"))
    .select("id", "country")
    .intersect(
        df_customers
        .filter(F.lower("email").like("%ran%"))
        .select("id", "country")
    )
)
