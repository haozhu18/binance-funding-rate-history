import os
import urllib.request
import base64
import json

def decode_b64(s: str) -> str:
    # Add padding if necessary, as Python's base64 is strict about it
    s = s.strip()
    return base64.b64decode(s + '=' * (-len(s) % 4)).decode('utf-8')

def main():
    sub_url = os.environ.get("JMS_SUB_URL")
    if not sub_url:
        raise ValueError("JMS_SUB_URL environment variable is missing.")

    # 1. Fetch the subscription data
    req = urllib.request.Request(sub_url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response:
        sub_data = response.read().decode('utf-8')

    # 2. Decode the main subscription list
    vmess_lines = [line for line in decode_b64(sub_data).splitlines() if line.startswith("vmess://")]
    
    # 3. Parse nodes and find Tokyo/s4
    chosen_node = None
    for line in vmess_lines:
        node_json = decode_b64(line[8:])
        node = json.loads(node_json)
        remark = node.get('ps', '').lower()
        
        # Look for server 4 (Japan)
        if 's4' in remark or 'tokyo' in remark:
            chosen_node = node
            break
            
    # Fallback to the last node in the list if specific ones aren't found
    if not chosen_node:
        chosen_node = json.loads(decode_b64(vmess_lines[-1][8:]))

    print(f"Selected Node: {chosen_node.get('ps', 'Unknown')}")

    # 4. Build the Xray config
    xray_config = {
        "inbounds": [{
            "port": 10809,
            "listen": "127.0.0.1",
            "protocol": "http" # We only need HTTP proxy for python requests/ccxt
        }],
        "outbounds": [{
            "protocol": "vmess",
            "settings": {
                "vnext": [{
                    "address": chosen_node['add'],
                    "port": int(chosen_node['port']),
                    "users": [{"id": chosen_node['id'], "alterId": int(chosen_node.get('aid', 0)), "security": "auto"}]
                }]
            },
            "streamSettings": {
                "network": chosen_node.get('net', 'tcp')
            }
        }]
    }

    # 5. Save it for the Xray core to use
    with open('xray_config.json', 'w') as f:
        json.dump(xray_config, f, indent=2)

if __name__ == "__main__":
    main()
