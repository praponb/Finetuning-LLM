from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("email").like("%@yahoo.com"))
    .select("city")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["London", "Berlin", "Chiang Mai"]))
        .select("city")
    )
    .orderBy("city")
)
