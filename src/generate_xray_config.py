import os
import urllib.request
import base64
import json
from urllib.parse import urlsplit, parse_qs, unquote

def decode_b64(s: str) -> str:
    s = s.strip()
    return base64.b64decode(s + '=' * (-len(s) % 4)).decode('utf-8')

def parse_vless(url: str) -> dict:
    parsed = urlsplit(url)
    params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    
    return {
        "id": parsed.username,
        "address": parsed.hostname,
        "port": parsed.port,
        "remark": unquote(parsed.fragment),
        "params": params
    }

def main():
    sub_url = os.environ.get("JMS_SUB_URL")
    if not sub_url:
        raise ValueError("JMS_SUB_URL environment variable is missing.")

    # 1. Fetch subscription data
    req = urllib.request.Request(sub_url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response:
        sub_data = response.read().decode('utf-8')

    # 2. Decode the subscription payload into individual links
    vless_lines = [
        line.strip() 
        for line in decode_b64(sub_data).splitlines() 
        if line.strip().startswith("vless://")
    ]

    if not vless_lines:
        raise ValueError("No VLESS nodes found in subscription.")

    # 3. Parse nodes and locate Tokyo / s4
    chosen_node = None
    for line in vless_lines:
        node = parse_vless(line)
        remark = node['remark'].lower()
        
        if 's4' in remark or 'tokyo' in remark:
            chosen_node = node
            break

    # Fallback to the last node if no match is found
    if not chosen_node:
        chosen_node = parse_vless(vless_lines[-1])

    print(f"Selected Node: {chosen_node['remark']}")

    # 4. Map transport and security settings
    params = chosen_node["params"]
    network_type = params.get("type", "tcp")
    security = params.get("security", "none")

    stream_settings = {
        "network": network_type,
        "security": security
    }

    # Populate TLS or REALITY settings if required by the node
    if security == "tls":
        stream_settings["tlsSettings"] = {
            "serverName": params.get("sni", chosen_node["address"])
        }
        if "alpn" in params:
            stream_settings["tlsSettings"]["alpn"] = params["alpn"].split(",")
    elif security == "reality":
        stream_settings["realitySettings"] = {
            "serverName": params.get("sni", ""),
            "publicKey": params.get("pbk", ""),
            "shortId": params.get("sid", ""),
            "spiderX": params.get("spx", "/")
        }

    # Populate transport-specific settings
    if network_type == "ws":
        stream_settings["wsSettings"] = {
            "path": params.get("path", "/"),
            "headers": {
                "Host": params.get("host", chosen_node["address"])
            }
        }
    elif network_type == "grpc":
        stream_settings["grpcSettings"] = {
            "serviceName": params.get("serviceName", "")
        }

    user_settings = {
        "id": chosen_node["id"],
        "encryption": params.get("encryption", "none")
    }
    if "flow" in params:
        user_settings["flow"] = params["flow"]

    # 5. Build Xray configuration
    xray_config = {
        "inbounds": [{
            "port": 10809,
            "listen": "127.0.0.1",
            "protocol": "http"
        }],
        "outbounds": [{
            "protocol": "vless",
            "settings": {
                "vnext": [{
                    "address": chosen_node["address"],
                    "port": int(chosen_node["port"]),
                    "users": [user_settings]
                }]
            },
            "streamSettings": stream_settings
        }]
    }

    # 6. Save config file
    with open('xray_config.json', 'w') as f:
        json.dump(xray_config, f, indent=2)

if __name__ == "__main__":
    main()
