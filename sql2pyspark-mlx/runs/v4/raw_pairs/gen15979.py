from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isin([14, 29]))
        .select("country")
    )
)
