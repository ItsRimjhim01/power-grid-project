"""
Task 12: Hadoop Task - HDFS Simulation
Task 13: HDFS Architecture - Namenode, Datanode simulation
Project: Power Grid Analytics Dashboard
Description: Simulate HDFS locally by organizing data as blocks.
             Explain Namenode/Datanode architecture.
"""

import os
import json
import hashlib
import math
import shutil
from datetime import datetime

print("=" * 60)
print("  Task 12 & 13: Hadoop HDFS Architecture Simulation")
print("=" * 60)

# -------------------------------------------------------
# HDFS Architecture Explanation
# -------------------------------------------------------
print("""
HDFS Architecture for Power Grid Project:
==========================================

  [Client Application (ETL / Spark)]
           |
           | (RPC calls)
           ↓
  ┌─────────────────────┐
  │     NAMENODE        │   ← Master node
  │  - Metadata store   │   ← Knows where every block lives
  │  - Namespace mgmt   │   ← Does NOT store actual data
  │  - Heartbeat check  │
  └─────────┬───────────┘
            │ Block locations
    ┌───────┼────────┐
    ↓       ↓        ↓
  [DN-1] [DN-2]   [DN-3]   ← DataNodes (workers)
  Block1  Block2   Block3   ← Store actual data blocks (128MB each)
  Block4  Block1   Block2   ← Replication factor = 3

For Power Grid:
  - power_data.csv → Split into 128MB blocks
  - Each block replicated 3× across DataNodes
  - Namenode tracks: file → [Block1(DN1,DN2), Block2(DN2,DN3)]
""")

# -------------------------------------------------------
# Simulate HDFS with local file system
# -------------------------------------------------------
HDFS_ROOT  = "hadoop/hdfs_sim"
NAMENODE   = f"{HDFS_ROOT}/namenode"
DATANODES  = [f"{HDFS_ROOT}/datanode_{i}" for i in range(1, 4)]
BLOCK_SIZE = 500  # 500 bytes per block for simulation

# Clean and recreate
shutil.rmtree(HDFS_ROOT, ignore_errors=True)
os.makedirs(NAMENODE, exist_ok=True)
for dn in DATANODES:
    os.makedirs(dn, exist_ok=True)
print("[+] HDFS directories created (Namenode + 3 DataNodes)")

# -------------------------------------------------------
# Simulate uploading a file to HDFS
# -------------------------------------------------------
def hdfs_put(local_path: str, hdfs_path: str):
    """Simulate uploading a file to HDFS with block splitting and replication."""
    print(f"\n[HDFS PUT] {local_path} → {hdfs_path}")

    with open(local_path, "rb") as f:
        content = f.read()

    total_size = len(content)
    num_blocks = math.ceil(total_size / BLOCK_SIZE)
    print(f"  File size   : {total_size} bytes")
    print(f"  Block size  : {BLOCK_SIZE} bytes")
    print(f"  Total blocks: {num_blocks}")

    metadata = {
        "hdfs_path"  : hdfs_path,
        "local_path" : local_path,
        "total_size" : total_size,
        "num_blocks" : num_blocks,
        "blocks"     : [],
        "uploaded_at": str(datetime.now())
    }

    for i in range(num_blocks):
        block_data = content[i * BLOCK_SIZE:(i + 1) * BLOCK_SIZE]
        block_id   = hashlib.md5(block_data).hexdigest()[:8]
        block_name = f"blk_{i:04d}_{block_id}"

        # Replicate across 3 datanodes (round-robin with replication)
        replicas = []
        for r, dn in enumerate(DATANODES):
            block_path = f"{dn}/{block_name}.dat"
            with open(block_path, "wb") as bf:
                bf.write(block_data)
            replicas.append(f"datanode_{r+1}")

        metadata["blocks"].append({
            "block_id" : block_name,
            "size"     : len(block_data),
            "replicas" : replicas
        })
        print(f"  Block {i:03d}: {block_name}  [{len(block_data)} bytes] → Replicated to {replicas}")

    # Save metadata to Namenode
    safe_name = hdfs_path.replace("/", "_").strip("_")
    meta_file = f"{NAMENODE}/{safe_name}.json"
    with open(meta_file, "w") as mf:
        json.dump(metadata, mf, indent=2)

    print(f"  Namenode metadata saved → {meta_file}")
    return metadata

# -------------------------------------------------------
# Simulate reading from HDFS
# -------------------------------------------------------
def hdfs_get(hdfs_path: str) -> bytes:
    """Simulate reading a file from HDFS by reassembling blocks."""
    print(f"\n[HDFS GET] Reading {hdfs_path}")
    safe_name = hdfs_path.replace("/", "_").strip("_")
    meta_file = f"{NAMENODE}/{safe_name}.json"

    with open(meta_file) as mf:
        meta = json.load(mf)

    content = b""
    for block in meta["blocks"]:
        dn_name    = block["replicas"][0]  # Read from first replica
        block_path = f"{HDFS_ROOT}/{dn_name}/{block['block_id']}.dat"
        with open(block_path, "rb") as bf:
            content += bf.read()

    print(f"  Reassembled {len(meta['blocks'])} blocks → {len(content)} bytes")
    return content

# -------------------------------------------------------
# Run simulation
# -------------------------------------------------------
SOURCE = "data/power_data.csv"
if not os.path.exists(SOURCE):
    # Create a small sample
    os.makedirs("data", exist_ok=True)
    with open(SOURCE, "w") as f:
        f.write("timestamp,region,consumption\n")
        from datetime import timedelta
        base = datetime(2026, 4, 1)
        import random
        for i in range(50):
            f.write(f"{base + timedelta(minutes=i)},North,{round(random.uniform(100,500),2)}\n")

meta = hdfs_put(SOURCE, "/powergrid/data/power_data.csv")
data = hdfs_get("/powergrid/data/power_data.csv")

print(f"\n  First 200 bytes of retrieved file:")
print(data[:200].decode(errors="replace"))

# -------------------------------------------------------
# HDFS ls simulation
# -------------------------------------------------------
print("\n[HDFS LS] /powergrid/data/")
print(f"  Found: /powergrid/data/power_data.csv")
print(f"  Size : {meta['total_size']} bytes | Blocks: {meta['num_blocks']} | Replication: 3")

print("\n✅ Task 12 & 13: Hadoop HDFS Simulation Complete.")
