from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .select("age", "name")
    .intersect(
        df_customers
        .filter(F.col("email").like("%@company.co"))
        .select("age", "name")
    )
    .orderBy("name")
)
