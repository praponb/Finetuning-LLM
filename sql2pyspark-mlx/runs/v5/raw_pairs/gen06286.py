from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("customer_id").isin([9, 25]))
        .select("country")
    )
    .distinct()
    .orderBy("country")
)
