from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("segment", "age")
    .unionByName(
        df_customers
        .filter(F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
        .select("segment", "age")
    )
)
