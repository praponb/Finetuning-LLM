from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 27]))
    .select("id", "opened_date")
    .intersect(
        df_accounts
        .filter(F.month("opened_date").isin([1, 6, 9]))
        .select("id", "opened_date")
    )
    .orderBy("id")
)
