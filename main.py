import psycopg2
from psycopg2.extras import execute_values
from prettytable import PrettyTable
import json
import random
import time

# ---------------------------
# PostgreSQL connection setup
# ---------------------------
conn = psycopg2.connect(
    host="localhost",
    database="test"
)
cur = conn.cursor()

# ---------------------------
# Helper functions
# ---------------------------
def recreate_tables():
    cur.execute("DROP TABLE IF EXISTS users, articles, documents;")
    conn.commit()
    cur.execute("""
    CREATE TABLE users (id serial PRIMARY KEY, name text, profile jsonb);
    CREATE TABLE articles (id serial PRIMARY KEY, title text, tags text[]);
    CREATE TABLE documents (id serial PRIMARY KEY, content text);
    """)
    conn.commit()
    print("Tables created.")

def insert_users(n=1000000, batch_size=10000):
    print(f"Inserting {n} users...")
    for i in range(0, n, batch_size):
        batch = [
            (
                f"user_{i+j}",
                json.dumps({
                    "prefs": {
                        "theme": "dark" if random.random() < 0.5 else "light",
                        "lang": "en" if random.random() < 0.5 else "fr"
                    }
                })
            )
            for j in range(batch_size)
        ]
        execute_values(cur, "INSERT INTO users (name, profile) VALUES %s", batch)
        conn.commit()
    print("Users inserted.")

def insert_articles(n=1000000, batch_size=10000):
    tags_list = ['tech', 'news', 'postgres', 'gin', 'json', 'sql', 'backend', 'tutorial']
    print(f"Inserting {n} articles...")
    for i in range(0, n, batch_size):
        batch = [
            (f"Article {i+j}", [random.choice(tags_list) for _ in range(5)])
            for j in range(batch_size)
        ]
        execute_values(cur, "INSERT INTO articles (title, tags) VALUES %s", batch)
        conn.commit()
    print("Articles inserted.")

def insert_documents(n=1000000, batch_size=10000):
    print(f"Inserting {n} documents...")
    for i in range(0, n, batch_size):
        batch = [(f"PostgreSQL GIN indexes are awesome. {random.getrandbits(128)}",)
                 for _ in range(batch_size)]
        execute_values(cur, "INSERT INTO documents (content) VALUES %s", batch)
        conn.commit()
    print("Documents inserted.")

# ---------------------------
# Index handling
# ---------------------------
def create_btree_indexes():
    print("Creating B-tree indexes...")
    cur.execute("CREATE INDEX idx_profile_btree ON users(profile);")
    cur.execute("CREATE INDEX idx_tags_btree ON articles(tags);")
    cur.execute("CREATE INDEX idx_content_btree ON documents(content);")
    conn.commit()
    print("B-tree indexes created.")

def create_gin_indexes():
    print("Creating GIN indexes...")
    cur.execute("CREATE INDEX idx_profile_gin ON users USING GIN (profile jsonb_path_ops);")
    cur.execute("CREATE INDEX idx_tags_gin ON articles USING GIN (tags);")
    cur.execute("CREATE INDEX idx_content_gin ON documents USING GIN (to_tsvector('english', content));")
    conn.commit()
    print("GIN indexes created.")

def drop_indexes(index_type="btree"):
    if index_type == "btree":
        cur.execute("DROP INDEX IF EXISTS idx_profile_btree, idx_tags_btree, idx_content_btree;")
    else:
        cur.execute("DROP INDEX IF EXISTS idx_profile_gin, idx_tags_gin, idx_content_gin;")
    conn.commit()
    print(f"{index_type.upper()} indexes dropped.")

# ---------------------------
# Metrics collection
# ---------------------------
def get_index_size(index_name):
    cur.execute(f"SELECT pg_total_relation_size('{index_name}')")
    return cur.fetchone()[0] / (1024*1024)  # convert to MB

def run_queries():
    queries = {
        "JSONB Query": "SELECT * FROM users WHERE profile @> '{\"prefs\": {\"theme\": \"dark\"}}';",
        "Array Query": "SELECT * FROM articles WHERE 'postgres' = ANY(tags);",
        "Full-Text Query": "SELECT * FROM documents WHERE to_tsvector('english', content) @@ to_tsquery('english', 'GIN & indexes');"
    }
    results = {}
    for query_name, sql in queries.items():
        cur.execute(f"EXPLAIN ANALYZE {sql}")
        explain_output = cur.fetchall()
        explain_text = "\n".join([row[0] for row in explain_output])
        # extract execution time
        for line in explain_text.split("\n"):
            if "Execution Time" in line:
                time_ms = float(line.strip().split(":")[1].replace(" ms",""))
                results[query_name] = time_ms
    return results

# ---------------------------
# Display results
# ---------------------------
def print_metrics(metrics):
    table = PrettyTable()
    table.field_names = ["Query", "B-tree (ms)", "B-tree size(MB)", "GIN (ms)", "GIN size(MB)", "Speedup (x)"]
    
    queries = metrics.keys()
    for q in queries:
        btree_time = metrics[q]["btree"]["time"]
        btree_size = metrics[q]["btree"]["size"]
        gin_time = metrics[q]["gin"]["time"]
        gin_size = metrics[q]["gin"]["size"]
        speedup = btree_time/gin_time if gin_time else 0
        table.add_row([q, f"{btree_time:.2f}", f"{btree_size:.2f}", f"{gin_time:.2f}", f"{gin_size:.2f}", f"{speedup:.2f}"])
    
    print(table)

# ---------------------------
# Main script
# ---------------------------
if __name__ == "__main__":
    # 1. Prepare data
    recreate_tables()
    insert_users(n=1000000, batch_size=10000)
    insert_articles(n=1000000, batch_size=10000)
    insert_documents(n=1000000, batch_size=10000)

    metrics = {}
    
    # 2. B-tree index metrics
    create_btree_indexes()
    btree_metrics = run_queries()
    metrics_btree_size = {
        "JSONB Query": get_index_size("idx_profile_btree"),
        "Array Query": get_index_size("idx_tags_btree"),
        "Full-Text Query": get_index_size("idx_content_btree")
    }
    drop_indexes("btree")
    
    # 3. GIN index metrics
    create_gin_indexes()
    gin_metrics = run_queries()
    metrics_gin_size = {
        "JSONB Query": get_index_size("idx_profile_gin"),
        "Array Query": get_index_size("idx_tags_gin"),
        "Full-Text Query": get_index_size("idx_content_gin")
    }
    drop_indexes("gin")
    
    # 4. Map metrics together
    for q in btree_metrics.keys():
        metrics[q] = {
            "btree": {"time": btree_metrics[q], "size": metrics_btree_size[q]},
            "gin": {"time": gin_metrics[q], "size": metrics_gin_size[q]}
        }

    # 5. Display and plot
    print_metrics(metrics)
    
    # Optional: save metrics to JSON
    with open("metrics.json", "w") as f:
        json.dump(metrics, f, indent=4)

    cur.close()
    conn.close()
