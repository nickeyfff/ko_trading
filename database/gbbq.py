from database.base import DuckDBBase


class Gbbq(DuckDBBase):
    def __init__(self):
        super().__init__()
        self.table_name = "gbbq"

    def query(self, code):
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE code = ?
        """
        params = [code]

        with self.conn.cursor() as cursor:
            df = cursor.execute(query, params).fetch_df()
        return df


gbbq = Gbbq()
