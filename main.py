import pandas as pd

db = {
    "customer_id": [23, 23, 23, 24, 25],
    "product_id": [1, 2, 1, 3, 4],
    "timestamp": [1668621480, 1668621481, 1668621482, 1668621483, 1568621484],
}


def fill_first_action(row: pd.DataFrame) -> int | None:
    """Добавить session_id для первого действия за сеанс"""
    global global_session_id
    temp_df = df[df["customer_id"] == row["customer_id"]]
    row_timestamp = row["timestamp"]
    temp_df = temp_df.query("timestamp > @row_timestamp - 179 & timestamp <= @row_timestamp")

    if (
        len(temp_df) == 1
        or temp_df["timestamp"].duplicated(keep=False).min()
        and row["product_id"] == temp_df["product_id"].min()
    ):
        global_session_id += 1
        return int(global_session_id)


def fill_nan(row: pd.DataFrame) -> int:
    """Заполнить session_id для второго и более действий за сеанс"""
    if pd.isnull(row["session_id"]) is True:
        temp_df = df[df["customer_id"] == row["customer_id"]]
        row_timestamp = row["timestamp"]
        temp_df = temp_df.query("timestamp > @row_timestamp - 179 & timestamp <= @row_timestamp")
        return int(temp_df["session_id"].min())
    else:
        return int(row["session_id"])


df = pd.DataFrame(db)

df = df.drop_duplicates()
df["session_id"] = None

global_session_id = 0

df["session_id"] = df.apply(fill_first_action, axis=1)
df["session_id"] = df.apply(fill_nan, axis=1)

print(df)
