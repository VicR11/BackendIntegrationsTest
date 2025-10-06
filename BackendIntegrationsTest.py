import requests
import re
from datetime import datetime

clientDataIpRank ={}
clientDataHostRank ={}
cantidadLineas =0
listaOrdenadaToRequest = []

url = "https://api.lumu.io/collectors/5ab55d08-ae72-4017-a41c-d9d735360288/dns/queries?key=d39a0f19-7278-4a64-a255-b7646d1ace80"

def detectar_tipo(linea: str):

    if re.search(r"\bIN\s+A\b", linea):
        return "A"
    elif re.search(r"\bIN\s+AAAA\b", linea):
        return "AAAA"
    else:
        return None

def escapar_texto(texto: str) -> str:

    reemplazos = {
        "\t": "\\t",
        "\r": "\\r",
        "\n": "\\n",
        "\\": "\\\\",
        '"': '\\"',
        "'": "\\'"
    }

    resultado = []
    for ch in texto:
        if ch in reemplazos:
            resultado.append(reemplazos[ch])
        elif 32 <= ord(ch) <= 126:
            resultado.append(ch)
        else:
            resultado.append(f"\\{ord(ch):03o}")
    return "".join(resultado)

with open("queries", "r", encoding="utf-8") as f:
    for linea in f:
        ipKey = linea.split()[6].split("#")[0]
        clientDataIpRank[ipKey] = clientDataIpRank.get(ipKey, 0)+1

        hostKey = linea.split()[7].split("#")[0].replace("(","").replace(")","")
        clientDataHostRank[hostKey] = clientDataHostRank.get(hostKey, 0)+1

        cantidadLineas +=1

        timestamp = datetime.strptime(linea.split("queries:")[0].strip(), "%d-%b-%Y %H:%M:%S.%f").isoformat()
        name = escapar_texto(linea.split()[7]).replace("(","").replace(")","").replace(":","")
        client_name = linea.split()[9]
        typeParametro = detectar_tipo(linea)

        listaOrdenadaToRequest.append({
            "timestamp": timestamp,
            "name": name,
            "client_ip": ipKey,
            "client_name": client_name,
            "type": typeParametro
        })


ordenado = dict(sorted(clientDataIpRank.items(), key=lambda x: x[1], reverse=True))
print(f"{'Client IPs Rank':<15}")
print(f"{'------------------':<25}{'-----':<8}{'-------':<10}")
for key in list(ordenado.keys())[:5]:
    print(f"{key:<25} {clientDataIpRank[key]:<8} {((clientDataIpRank[key]*100)/cantidadLineas):.2f}%")
print(f"{'------------------':<25}{'-----':<8}{'-------':<10}")

ordenado = dict(sorted(clientDataHostRank.items(), key=lambda x: x[1], reverse=True))

print(f"{'Host Rank':<55}")
print(f"{'---------------------------------------------':<55}{'-----':<10}{'-----':<10}")
for key in list(ordenado.keys())[:5]:
     print(f"{key:<55} {clientDataHostRank[key]:<8} {((clientDataHostRank[key]*100)/cantidadLineas):.2f}%")
print(f"{'---------------------------------------------':<55}{'-----':<10}{'-----':<10}")

def chunks(lista, tamaño):
    for i in range(0, len(lista), tamaño):
        yield lista[i:i+tamaño]


for bloque in chunks(listaOrdenadaToRequest, 500):
    response = requests.post(url, json=bloque)
    print("Código de estado:", response.status_code)


       




