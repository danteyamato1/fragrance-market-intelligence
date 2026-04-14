import sqlite3
conn = sqlite3.connect("fragrance_market.db")
c = conn.cursor()
c.execute("DELETE FROM fragrantica_data")
c.execute("DELETE FROM amazon_prices")
c.execute("DELETE FROM fragrances")
c.execute("DELETE FROM brands")
conn.commit()
conn.close()
print("All tables cleared.")

