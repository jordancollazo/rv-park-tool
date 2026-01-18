import os

STATIC_PATH = 'output/opportunity_map.html'
DYNAMIC_PATH = '.tmp/dynamic_map.html'

def compare():
    with open(STATIC_PATH, 'r', encoding='utf-8') as f:
        static = f.read()
    with open(DYNAMIC_PATH, 'r', encoding='utf-8') as f:
        dynamic = f.read()

    print(f"Static len: {len(static)}")
    print(f"Dynamic len: {len(dynamic)}")
    print(f"ID map in static: {'id=\"map\"' in static}")
    print(f"ID map in dynamic: {'id=\"map\"' in dynamic}")
    print(f"L.map in static: {'L.map' in static}")
    print(f"L.map in dynamic: {'L.map' in dynamic}")
    print(f"L.tileLayer in static: {'L.tileLayer' in static}")
    print(f"L.tileLayer in dynamic: {'L.tileLayer' in dynamic}")
    
    # Check if scripts are present
    print(f"Script tag in static: {'<script>' in static}")
    print(f"Script tag in dynamic: {'<script>' in dynamic}")

    # Show first difference location
    min_len = min(len(static), len(dynamic))
    for i in range(min_len):
        if static[i] != dynamic[i]:
            context_start = max(0, i - 50)
            context_end = min(min_len, i + 50)
            print(f"First diff at char {i}:")
            print(f"  Static: {repr(static[context_start:context_end])}")
            print(f"  Dynamic: {repr(dynamic[context_start:context_end])}")
            break
    else:
        if len(static) != len(dynamic):
            print("Files differ only in length.")
        else:
            print("Files are identical.")

if __name__ == "__main__":
    compare()
