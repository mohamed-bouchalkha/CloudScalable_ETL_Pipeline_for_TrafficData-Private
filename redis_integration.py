import redis
import hashlib
import json
import os
from dotenv import load_dotenv
import time

# Charger les variables d'environnement
load_dotenv()

# Configuration Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")  # Vide par défaut
REDIS_TTL = int(os.getenv("REDIS_TTL", 86400))  # TTL par défaut: 24 heures (en secondes)

# Variable pour éviter d'afficher l'erreur Redis en continu
_redis_warning_displayed = False

# Création d'une connexion Redis
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD if REDIS_PASSWORD else None,  # None si vide
        decode_responses=True,
        socket_timeout=1,  # Timeout court pour éviter de bloquer l'application
        socket_connect_timeout=1
    )
    # Tester la connexion dès le démarrage
    redis_client.ping()
    print("✅ Connexion à Redis établie avec succès")
except redis.ConnectionError:
    print("⚠️ Impossible de se connecter à Redis au démarrage, la déduplication sera désactivée")
    _redis_warning_displayed = True

def generate_data_hash(traffic_data):
    """
    Génère un hash unique pour les données de trafic.
    Le hash est basé sur les coordonnées et les données essentielles du trafic.
    """
    flow_data = traffic_data.get("flowSegmentData", {})
    
    # Extraire les données clés pour le hachage
    key_fields = {
        "frc": flow_data.get("frc"),
        "currentSpeed": flow_data.get("currentSpeed"),
        "freeFlowSpeed": flow_data.get("freeFlowSpeed"),
        "coordinates": flow_data.get("coordinates", {}).get("coordinate", [])
    }
    
    # Convertir les données en chaîne JSON triée pour assurer la cohérence du hachage
    data_str = json.dumps(key_fields, sort_keys=True)
    
    # Générer le hash
    return hashlib.md5(data_str.encode()).hexdigest()

def is_data_duplicate(traffic_data):
    """
    Vérifie si les données sont déjà présentes dans le cache Redis.
    Retourne True si les données sont un doublon, False sinon.
    """
    # Si Redis n'est pas disponible, considérer qu'il n'y a pas de doublon
    if not is_redis_available():
        return False
    
    try:
        data_hash = generate_data_hash(traffic_data)
        key_name = f"traffic:data:{data_hash}"
        
        # Vérifier si le hash existe dans Redis
        exists = redis_client.exists(key_name)
        
        if not exists:
            # Si le hash n'existe pas, l'ajouter avec un TTL
            redis_client.set(key_name, "1", ex=REDIS_TTL)
        
        return bool(exists)
    except Exception:
        # En cas d'erreur, continuer le traitement
        return False

def cache_data_entry(traffic_data_id):
    """
    Cache l'ID des données de trafic insérées dans la base de données.
    Cela permet de suivre quelles données ont été enregistrées.
    """
    if not is_redis_available():
        return
    
    try:
        key_name = f"traffic:saved:{traffic_data_id}"
        redis_client.set(key_name, "1", ex=REDIS_TTL)
    except Exception:
        # Ignorer les erreurs lors du cache
        pass

def is_redis_available():
    """
    Vérifie si Redis est disponible.
    """
    global _redis_warning_displayed
    
    try:
        return redis_client.ping()
    except (redis.ConnectionError, redis.exceptions.TimeoutError, redis.exceptions.RedisError):
        # N'afficher l'avertissement qu'une fois toutes les 5 minutes
        current_time = time.time()
        if not hasattr(is_redis_available, "last_warning_time") or current_time - getattr(is_redis_available, "last_warning_time", 0) > 300:
            if not _redis_warning_displayed:
                print("⚠️ Impossible de se connecter à Redis, la déduplication sera désactivée")
                _redis_warning_displayed = True
            is_redis_available.last_warning_time = current_time
        return False