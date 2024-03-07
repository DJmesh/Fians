import paho.mqtt.client as mqtt
import pymysql
import json
import time
import pytz
from datetime import datetime
import requests

def get_brazil_current_time():
    try:
        brazil_timezone = pytz.timezone('America/Sao_Paulo')
        brazil_datetime = datetime.now(brazil_timezone)
        return brazil_datetime.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print("Erro ao obter o horário do Brasil:", e)
        return None
    
def convert_to_brazilian_timezone(utc_datetime_str):
    # Remover o sufixo 'Z' da string de data e hora
    utc_datetime_str = utc_datetime_str.replace('Z', '')
    
    brazil_datetime_str = get_brazil_current_time()
    if brazil_datetime_str:
        # Converte a string de data e hora para um objeto datetime
        brazil_datetime = datetime.strptime(utc_datetime_str, '%Y-%m-%dT%H:%M:%S')
        return brazil_datetime
    else:
        return None
# Buffer para armazenar os dados recebidos
data_buffer = []

# Inicializa a conexão com o banco de dados fora das funções
db = pymysql.connect(host='54.221.62.133', user='smartdb', password='Smart2024', database='SmartFloods')
cursor = db.cursor()

def on_connect(client, userdata, flags, rc):
    print("Conectado com código de resultado: " + str(rc))
    client.subscribe("Floods")

def interpret_hex_data(hex_data):
    try:
        return bytes.fromhex(hex_data).decode('ascii')
    except UnicodeDecodeError:
        return "Dados em hexadecimal não decodificáveis em ASCII"

# Modifique a função on_message para usar a função de conversão de fuso horário
def on_message(client, userdata, msg):
    try:
        json_data = json.loads(msg.payload.decode())
        print("JSON recebido:", json_data)
        
        if json_data.get("devaddr") == 'F3E35972' and "data" in json_data:
            ascii_message = interpret_hex_data(json_data["data"])
            print("Mensagem interpretada:", ascii_message)
            json_data["data_interpretada"] = ascii_message
            
            
            json_data["datetime_brazil"] = convert_to_brazilian_timezone(json_data["datetime"])
            if json_data["datetime_brazil"]:
                print("Datetime no fuso horário do Brasil:", json_data["datetime_brazil"])
                
                
                data_buffer.append(json_data)
            else:
                print("Não foi possível obter o horário do Brasil. Dados não adicionados ao buffer.")
            
    except json.JSONDecodeError as e:
        print("Erro ao decodificar JSON: ", e)
        
def send_data_from_buffer():
    while data_buffer:
        json_data = data_buffer.pop(0)
        # Extrai e prepara os dados para inserção
        info = json_data['data_interpretada'].split(' | ')
        dados = {x.split(' ')[0]: x.split(' ')[1] for x in info}
        
        # Certifique-se de que todos os valores, incluindo datetime, estejam sendo passados corretamente
        cursor.execute('''INSERT INTO SmartFloods (distancia, temperatura, umidade, chuva, create_at) 
                        VALUES (%s, %s, %s, %s, %s)''',
                       (dados.get('dist'), dados.get('temp'), dados.get('umi'), dados.get('chuva'), json_data['datetime_brazil']))
        db.commit()
        print("Dados inseridos com sucesso no banco de dados.")

    # Aguarda 2 minutos antes de enviar os próximos dados
    time.sleep(30)
    send_data_from_buffer()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("52.1.135.87", 1883, 60)
client.loop_start()  # Inicia o loop em uma thread separada

try:
    send_data_from_buffer()  # Inicia o envio de dados do buffer
except KeyboardInterrupt:
    print("Encerrando...")
    client.disconnect()
    db.close()
