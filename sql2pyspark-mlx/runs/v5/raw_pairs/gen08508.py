from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .intersect(
        df_stores
        .filter(F.col("city").isin(["London", "Berlin", "Bangkok"]))
        .select("city")
    )
    .orderBy("city")
)
