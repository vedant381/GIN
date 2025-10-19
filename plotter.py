import json
import matplotlib.pyplot as plt

def plot_metrics(metrics_file="metrics.json"):
    """
    Reads performance metrics from a JSON file and plots a comparison chart.
    """
    try:
        with open(metrics_file, "r") as f:
            metrics = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{metrics_file}' was not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from the file '{metrics_file}'.")
        return

    queries = list(metrics.keys())
    if not queries:
        print("No data to plot.")
        return

    btree_times = [metrics[q]["btree"]["time"] for q in queries]
    gin_times = [metrics[q]["gin"]["time"] for q in queries]

    x = range(len(queries))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.bar(x, btree_times, width, label="B-tree", color="salmon")
    ax.bar([i + width for i in x], gin_times, width, label="GIN", color="skyblue")
    
    ax.set_ylabel("Execution Time (ms)")
    ax.set_title("B-tree vs GIN Performance Comparison")
    ax.set_xticks([i + width / 2 for i in x])
    ax.set_xticklabels(queries, rotation=15, ha="right")
    ax.legend()
    
    plt.tight_layout()
    plt.savefig("index_comparison_from_json.png")
    print("Chart saved as 'index_comparison_from_json.png'")
    plt.show()

if __name__ == "__main__":
    plot_metrics()
