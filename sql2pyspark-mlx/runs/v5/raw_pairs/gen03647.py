from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") <= 61)
    .select("country")
    .unionByName(
        df_suppliers
        .filter(F.col("city").isin(["New York", "Chiang Mai", "London"]))
        .select("country")
    )
    .distinct()
    .orderBy("country")
)
