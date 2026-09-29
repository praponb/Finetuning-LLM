from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co"))
    .select("id", "age", "name")
    .unionByName(
        df_customers
        .filter(F.lower("email").like("%ran%"))
        .select("id", "age", "name")
    )
    .orderBy("id")
)
