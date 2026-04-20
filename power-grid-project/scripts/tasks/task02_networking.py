"""
Task 2: Networking Task
Project: Power Grid Analytics Dashboard
Description: Simulate data transfer between systems using APIs.
             Analyze HTTP/FTP protocols and explain secure data flow.
"""

import requests
import json
import time
import socket
import ssl
import urllib.request
from datetime import datetime

BASE_URL = "http://127.0.0.1:5000"

# -------------------------------------------------------
# Section A: HTTP Protocol Analysis
# -------------------------------------------------------
print("=" * 60)
print("  Task 2: Networking - API Data Transfer Simulation")
print("=" * 60)

def test_api_endpoint(url, label):
    """Test an API endpoint and display HTTP metadata."""
    print(f"\n[+] Testing: {label}")
    print(f"    URL: {url}")
    try:
        start = time.time()
        response = requests.get(url, timeout=5)
        elapsed = round((time.time() - start) * 1000, 2)

        print(f"    Protocol   : HTTP/1.1")
        print(f"    Status Code: {response.status_code}")
        print(f"    Response   : {elapsed} ms")
        print(f"    Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        print(f"    Data Sample: {str(response.json())[:120]}...")
    except requests.exceptions.ConnectionError:
        print(f"    [OFFLINE] Backend server is not running. Simulating response.")
        sample = {
            "/data": [{"timestamp": str(datetime.now()), "region": "North", "consumption": 280.5}],
            "/region": [{"region": "North", "avg_usage": 290.3}],
            "/health": {"status": "OK", "message": "Server running", "time": str(datetime.now())}
        }
        endpoint = url.replace(BASE_URL, "")
        print(f"    Simulated Response: {json.dumps(sample.get(endpoint, {}))[:120]}")
    except Exception as e:
        print(f"    Error: {e}")

# Test all API endpoints
test_api_endpoint(f"{BASE_URL}/health", "Health Check API")
test_api_endpoint(f"{BASE_URL}/data",   "Power Data API")
test_api_endpoint(f"{BASE_URL}/region", "Region-wise Data API")

# -------------------------------------------------------
# Section B: HTTP vs FTP Protocol Comparison
# -------------------------------------------------------
print("\n" + "=" * 60)
print("  Protocol Comparison: HTTP vs FTP")
print("=" * 60)

comparison = {
    "Feature"     : ["Protocol Type", "Port", "Statefulness", "Security",      "Use Case in Project"],
    "HTTP"        : ["Application",   "80",   "Stateless",    "HTTPS (TLS)",   "API calls to backend"],
    "FTP"         : ["File Transfer", "21",   "Stateful",     "FTPS / SFTP",   "Bulk CSV upload to server"],
}

print(f"\n{'Feature':<25} {'HTTP':<30} {'FTP':<30}")
print("-" * 85)
for i in range(5):
    print(f"{comparison['Feature'][i]:<25} {comparison['HTTP'][i]:<30} {comparison['FTP'][i]:<30}")

# -------------------------------------------------------
# Section C: How Data Flows Securely
# -------------------------------------------------------
print("\n" + "=" * 60)
print("  Secure Data Flow Explanation")
print("=" * 60)
print("""
Data Flow in Power Grid Analytics:

  [IoT Sensor / CSV Source]
        ↓  (File system / FTP)
  [ETL Script - Python]
        ↓  (MySQL INSERT over local socket)
  [MySQL Database]
        ↓  (SQL Query via mysql2 driver)
  [Node.js Backend API - Port 5000]
        ↓  (HTTP GET request with CORS)
  [React Frontend Dashboard]
        ↓  (Browser renders charts)
  [End User]

Security Measures:
  ✅ CORS headers restrict cross-origin access
  ✅ Parameterized SQL queries prevent SQL injection
  ✅ Environment variables for DB credentials (not hardcoded in prod)
  ✅ HTTPS/TLS can be enabled using SSL certificates
  ✅ JWT tokens can be added for API authentication
""")

# -------------------------------------------------------
# Section D: Simulate packet capture summary
# -------------------------------------------------------
print("=" * 60)
print("  Simulated Packet Capture Summary (HTTP GET /data)")
print("=" * 60)
print("""
  Frame 1: [SYN]      Client → Server (127.0.0.1:RANDOM → 127.0.0.1:5000)
  Frame 2: [SYN-ACK]  Server → Client (TCP Handshake)
  Frame 3: [ACK]      Client → Server
  Frame 4: [HTTP GET] GET /data HTTP/1.1
                       Host: 127.0.0.1:5000
                       Accept: application/json
  Frame 5: [HTTP 200] Server → Client
                       Content-Type: application/json
                       Body: [{timestamp: ..., region: ..., consumption: ...}]
  Frame 6: [FIN]      Connection closed
""")

print("✅ Task 2: Networking Analysis Complete.")
