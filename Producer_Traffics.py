import requests
from kafka import KafkaProducer
import json
import time
import random
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from redis_integration import is_data_duplicate, is_redis_available

# Charger les variables d'environnement du fichier .env
load_dotenv()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 8))  # Définit 8 comme valeur par défaut


# Vérifiez si la variable d'environnement est définie
if KAFKA_TOPIC is None:
    print("❌ La variable d'environnement KAFKA_TOPIC n'est pas définie !")
    exit(1)

# Création d'un producteur Kafka global
producer = KafkaProducer(
    bootstrap_servers=[os.getenv("KAFKA_SERVER")],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fonction pour envoyer les données dans Kafka (en utilisant un batch)
def send_to_kafka(topic, data):
    try:
        producer.send(topic, data)
        print("✅ Données envoyées dans Kafka.")
    except Exception as e:
        print(f"⚠️ Erreur lors de l'envoi de données dans Kafka: {e}")

# 📂 Charger les routes depuis un fichier JSON
def load_roads_from_file(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("roads", [])
    except FileNotFoundError:
        print(f"❌ Fichier {filename} non trouvé !")
        return []
    except json.JSONDecodeError:
        print(f"❌ Erreur lors de la lecture du fichier JSON {filename}.")
        return []

# 🔍 Fonction pour récupérer l'état du trafic via TomTom
def get_traffic_status(lat, lon):
    api_key = os.getenv("TOMTOM_API_KEY")
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={api_key}&point={lat},{lon}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"⚠️ Erreur API TomTom: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Erreur réseau: {e}")
        return None

def process_road_file(filename):
    print(f"Traitement du fichier {filename}...")
    roads = load_roads_from_file(filename)
    if not roads:
        return

    main_roads = [r for r in roads if r["properties"].get("highway_type") in 
                 ["primary", "secondary", "trunk", "motorway"]]
    
    if not main_roads:
        main_roads = roads  # Fallback si le filtrage ne donne rien
    
    print(f"Monitoring {len(main_roads)} routes principales sur {len(roads)} routes totales dans {filename}.")

    # Traitement des routes dans un thread séparé
    futures = []
    for selected_road in main_roads:
        if not selected_road["location"]["coordinates"]:
            continue
        
        middle_idx = len(selected_road["location"]["coordinates"]) // 2
        points_to_check = [
            selected_road["location"]["coordinates"][0],  # Début
            selected_road["location"]["coordinates"][middle_idx],  # Milieu
            selected_road["location"]["coordinates"][-1]  # Fin
        ]

        # Soumettre des tâches pour chaque point à vérifier
        for point in points_to_check:
            lat, lon = point["lat"], point["lon"]
            futures.append(executor.submit(process_traffic_data, lat, lon))

    return futures
# Importer le module Redis dans votre producer.py
from redis_integration import is_data_duplicate, is_redis_available

# Modifier la fonction process_traffic_data dans producer.py
def process_traffic_data(lat, lon):
    traffic_info = get_traffic_status(lat, lon)
    
    # Ne traiter que si les informations de trafic sont disponibles
    if traffic_info:
        # Vérifier les fermetures de route
        road_closed = traffic_info.get("flowSegmentData", {}).get("roadClosure") == True
        
        # Vérifier si les données ne sont pas des doublons si Redis est disponible
        redis_available = is_redis_available()
        is_duplicate = redis_available and is_data_duplicate(traffic_info)
        
        # Si c'est une fermeture de route ou si la déduplication est désactivée ou si ce n'est pas un doublon
        if road_closed or not redis_available or not is_duplicate:
            if road_closed:
                print("🚨 FERMETURE DE ROUTE DÉTECTÉE! 🚨")
            elif is_duplicate:
                print("⏩ Ignorer les données en double grâce à Redis")
                return
            
            # Envoyer les données à Kafka
            send_to_kafka(KAFKA_TOPIC, traffic_info)

# 🔄 Exécution du script avec multithreading
if __name__ == "__main__":
    # Directory contenant les fichiers JSON
    directory = r"C:\Users\hp\Desktop\TrafficsSystem project\ScrapingTrafficsData\RoadsData"

    # Obtenir tous les fichiers JSON dans le répertoire
    files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith(".json")]

    # Utiliser ThreadPoolExecutor pour traiter les fichiers en parallèle
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Créer une liste de tâches
        tasks = [executor.submit(process_road_file, file) for file in files]
        
        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                print(f"Erreur dans le traitement du fichier: {e}")

    # Assurer l'envoi de tous les messages en attente avant de terminer
    producer.flush()
    # Fermer proprement le producteur Kafka
    producer.close()
    print("🔄 Producteur Kafka fermé proprement.")