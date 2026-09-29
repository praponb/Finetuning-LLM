from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS", "CREDIT"]))
    .select("customer_id", "status")
    .intersect(
        df_accounts
        .filter(F.month("opened_date").isin([1, 5, 8]))
        .select("customer_id", "status")
    )
    .orderBy("customer_id")
)
