import paho.mqtt.client as mqtt
import json
import pymysql
import pytz
from datetime import datetime
import time

# Configuração inicial do banco de dados
try:
    db = pymysql.connect(host='54.221.62.133', user='smartdb', password='Smart2024', database='SmartFloods')
    cursor = db.cursor()
    print("Conexão com o banco de dados estabelecida com sucesso.")
    # Cria a tabela SmartFloods se não existir
    cursor.execute('''CREATE TABLE IF NOT EXISTS SmartFloods (
                    id INT AUTO_INCREMENT PRIMARY KEY, 
                    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
                    distancia VARCHAR(255), 
                    temperatura VARCHAR(255), 
                    umidade VARCHAR(255), 
                    chuva VARCHAR(255), 
                    direcao_do_vento VARCHAR(255) DEFAULT NULL, 
                    velocidade_do_vento VARCHAR(255) DEFAULT NULL)''')
except pymysql.err.OperationalError as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

# Buffer para combinar os dados recebidos
combined_data_buffer = {}

# Mapa de direções do vento
wind_direction_map = {
    1: "Norte",
    2: "Leste",
    3: "Sul",
    4: "Oeste",
    5: "Nordeste",
    6: "Noroeste",
    7: "Sudeste",
    8: "Sudoeste"
}

# Funções para conversão de tempo
def get_brazil_current_time():
    try:
        brazil_timezone = pytz.timezone('America/Sao_Paulo')
        brazil_datetime = datetime.now(brazil_timezone)
        return brazil_datetime.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print("Erro ao obter o horário do Brasil:", e)
        return None

def convert_to_brazilian_timezone(utc_datetime_str):
    utc_datetime_str = utc_datetime_str.replace('Z', '')
    brazil_datetime_str = get_brazil_current_time()
    if brazil_datetime_str:
        brazil_datetime = datetime.strptime(utc_datetime_str, '%Y-%m-%dT%H:%M:%S')
        return brazil_datetime
    else:
        return None

# Callbacks do cliente MQTT
def on_connect(client, userdata, flags, rc):
    print("Conectado com código de resultado: " + str(rc))
    client.subscribe("Floods")

def interpret_hex_data(hex_data):
    try:
        bytes_data = bytes.fromhex(hex_data)
        ascii_data = bytes_data.decode('ascii')
        return ascii_data
    except UnicodeDecodeError:
        return "Dados em hexadecimal não decodificáveis em ASCII"

def send_data_from_buffer():
    # Implementação da função send_data_from_buffer
    pass

def on_message(client, userdata, msg):
    try:
        json_data = json.loads(msg.payload.decode())
        #print("JSON recebido:", json_data)
        
        if json_data.get("devaddr") == '1914FACE':
            ascii_message = interpret_hex_data(json_data["data"])
            #print("Mensagem interpretada:", ascii_message)
            
            interpreted_json_data = json.loads(ascii_message)
            if "dir" in interpreted_json_data and isinstance(interpreted_json_data["dir"], int):
                dir_code = interpreted_json_data["dir"]
                interpreted_json_data["dir"] = wind_direction_map.get(dir_code, "Desconhecido")
            
            dev_id = json_data["devaddr"]
            frame_count = json_data["fcnt"]
            combined_data = combined_data_buffer.setdefault((dev_id, frame_count), {})
            
            combined_data.update(interpreted_json_data)
            combined_data['datetime_brazil'] = get_brazil_current_time()
            
            # Verificamos se já temos todos os dados necessários para enviar ao banco
            if 'dist' in combined_data and 'umi' in combined_data:
                send_data_to_database(dev_id, frame_count)
                
        else:
            print("Ignorando mensagem de outro dispositivo.")
            
    except json.JSONDecodeError as e:
        print("Erro ao decodificar JSON: ", e)

def send_data_to_database(dev_id, frame_count):
    data_to_insert = combined_data_buffer.pop((dev_id, frame_count), None)
    if data_to_insert:
        merged_data = {
            'umi': data_to_insert.get('umi', 0),
            'temp': data_to_insert.get('temp', 0),
            'chu': data_to_insert.get('chu', 0),
            'dist': data_to_insert.get('dist', 0),
            'velo': data_to_insert.get('velo', 0),
            'dir': data_to_insert.get('dir', 'Desconhecido'),
            'datetime_brazil': data_to_insert.get('datetime_brazil', None)
        }
        print(merged_data)

        try:
            cursor.execute('''INSERT INTO SmartFloods (distancia, umidade, temperatura, chuva, direcao_do_vento, velocidade_do_vento, create_at) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                            (merged_data['dist'], merged_data['umi'], merged_data['temp'], merged_data['chu'],
                             merged_data['dir'], merged_data['velo'], merged_data['datetime_brazil']))
            db.commit()
            print("Dados inseridos com sucesso no banco de dados:", merged_data)
        except pymysql.MySQLError as e:
            print(f"Erro ao inserir no banco de dados: {e}")
            # Se houver erro na inserção, considerar recolocar o item no buffer ou tratar de outra forma.

# Inicialização e loop do cliente MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("52.1.135.87", 1883, 60)
client.loop_start()

try:
    while True:
        send_data_from_buffer()
        time.sleep(30)  # Intervalo de tempo entre as verificações do buffer
except KeyboardInterrupt:
    print("Encerrando...")
    client.disconnect()
    db.close()
