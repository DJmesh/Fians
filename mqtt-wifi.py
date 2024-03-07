import paho.mqtt.client as mqtt

# Callback chamado quando o cliente recebe uma resposta CONNACK do servidor.
def on_connect(client, userdata, flags, rc):
    print("Conectado com o código de resultado "+str(rc))
    # Inscrevendo no tópico no on_connect significa que se perdermos a conexão e
    # reconectarmos então as subscrições serão renovadas.
    client.subscribe("sensor/data")

# Callback chamado quando uma mensagem PUBLISH é recebida do servidor.
def on_message(client, userdata, msg):
    print("Tópico: "+msg.topic+" Mensagem: "+str(msg.payload))

# Configuração do cliente MQTT.
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Configure aqui suas credenciais de conexão MQTT.
client.username_pw_set('seu_usuario', 'sua_senha') # Opcional, depende do seu servidor

client.connect("52.1.135.87", 1883, 60)

# Loop de processamento de mensagens. Bloqueia o programa neste loop.
client.loop_forever()
