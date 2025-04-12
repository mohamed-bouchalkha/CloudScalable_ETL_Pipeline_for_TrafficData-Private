import asyncio
import json
import os
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from multiprocessing import Pool
import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition, OffsetAndMetadata
from dotenv import load_dotenv
from redis_integration import is_data_duplicate, cache_data_entry, is_redis_available

# Charger les variables d'environnement
load_dotenv()

# Variables globales pour le monitoring
start_time = time.time()
messages_processed = 0
batches_processed = 0
db_errors = 0
kafka_errors = 0

# Configuration de la base de données
DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Configuration Kafka
KAFKA_SERVERS = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_CONSUMER_GROUP = "traffic_consumer_group"
KAFKA_TOPIC_INPUT = "traffic_data"
KAFKA_TOPIC_SUCCESS = "traffic_data_saved"
KAFKA_TOPIC_ERRORS = "traffic_data_errors"

async def create_db_pool():
    """Crée un pool de connexions PostgreSQL asynchrone"""
    return await asyncpg.create_pool(**DB_CONFIG)

async def prepare_batch_data(messages: List[Dict]) -> Tuple[List, List]:
    """Prépare les données pour l'insertion en batch de manière asynchrone"""
    traffic_data_values = []
    coordinates_mapping = []

    for msg in messages:
        flow_data = msg.get("flowSegmentData", {})
        
        # Préparer les données de trafic
        traffic_values = (
            flow_data.get("frc"),
            flow_data.get("currentSpeed"),
            flow_data.get("freeFlowSpeed"),
            flow_data.get("currentTravelTime"),
            flow_data.get("freeFlowTravelTime"),
            flow_data.get("confidence"),
            flow_data.get("roadClosure", False)
        )
        traffic_data_values.append(traffic_values)

        # Stocker les coordonnées avec l'index correspondant
        coordinates = flow_data.get("coordinates", {}).get("coordinate", [])
        coordinates_mapping.append({
            'index': len(traffic_data_values) - 1,
            'coordinates': coordinates
        })

    return traffic_data_values, coordinates_mapping

# Importer le module Redis dans votre consumer.py
from redis_integration import is_data_duplicate, cache_data_entry, is_redis_available

async def save_to_postgres_batch(
    messages: List[Dict], 
    producer: AIOKafkaProducer, 
    db_pool: asyncpg.Pool
) -> bool:
    """Sauvegarde un batch de messages dans PostgreSQL de manière asynchrone avec déduplication Redis"""
    global messages_processed, batches_processed, db_errors
    
    if not messages:
        return True
    
    # Vérifier si Redis est disponible
    redis_available = is_redis_available()
    
    # Filtrer les messages pour éliminer les doublons si Redis est disponible
    unique_messages = []
    if redis_available:
        for msg in messages:
            if not is_data_duplicate(msg):
                unique_messages.append(msg)
        
        # Journaliser le nombre de doublons détectés
        duplicates_count = len(messages) - len(unique_messages)
        if duplicates_count > 0:
            print(f"🔄 {duplicates_count} doublons détectés et ignorés grâce à Redis")
        
        # Mettre à jour les messages à traiter
        messages = unique_messages
        
        # Si tous les messages sont des doublons, retourner succès sans traitement
        if not messages:
            print("✅ Tous les messages étaient des doublons, rien à traiter")
            return True
    
    async with db_pool.acquire() as conn:
        tr = conn.transaction()
        try:
            await tr.start()
            start_time = time.time()

            # Préparer les données
            traffic_data_values, coordinates_mapping = await prepare_batch_data(messages)

            # Insérer les données de trafic en batch et récupérer les IDs
            traffic_data_ids = await conn.fetch(
                """
                INSERT INTO traffic_data 
                (frc, current_speed, free_flow_speed, current_travel_time, 
                free_flow_travel_time, confidence, road_closure)
                SELECT frc, current_speed, free_flow_speed, current_travel_time, 
                free_flow_travel_time, confidence, road_closure
                FROM UNNEST($1::traffic_data_params[])
                RETURNING id
                """,
                traffic_data_values
            )
            
            # Préparation des données de coordonnées avec les IDs associés
            coordinates_values = []
            for coord_map in coordinates_mapping:
                traffic_data_id = traffic_data_ids[coord_map['index']]['id']
                
                for i, coord in enumerate(coord_map['coordinates']):
                    coordinates_values.append((
                        traffic_data_id,
                        coord.get('latitude'),
                        coord.get('longitude'),
                        i  # ordre
                    ))
            
            # Insérer les coordonnées en batch
            if coordinates_values:
                await conn.executemany(
                    """
                    INSERT INTO traffic_coordinates 
                    (traffic_data_id, latitude, longitude, coordinate_order)
                    VALUES ($1, $2, $3, $4)
                    """,
                    coordinates_values
                )
            
            # Finaliser la transaction
            await tr.commit()
            
            # Mise à jour des compteurs
            messages_processed += len(messages)
            batches_processed += 1
            
            # Ajouter les IDs dans Redis pour le suivi si Redis est disponible
            if redis_available:
                for id_record in traffic_data_ids:
                    cache_data_entry(id_record['id'])
            
            # Envoyer un message de confirmation au topic de succès
            # Ceci est optionnel, mais permet de suivre le traitement réussi
            for msg in messages:
                await producer.send_and_wait(
                    KAFKA_TOPIC_SUCCESS,
                    json.dumps({
                        "status": "success",
                        "timestamp": datetime.now().isoformat(),
                        "message_id": msg.get("id", "unknown")
                    }).encode('utf-8')
                )
            
            processing_time = time.time() - start_time
            print(f"✅ Batch enregistré dans PostgreSQL en {processing_time:.3f}s")
            
            return True
            
        except Exception as e:
            await tr.rollback()
            db_errors += 1
            error_details = str(e)
            
            print(f"❌ Erreur lors de l'enregistrement dans PostgreSQL: {error_details}")
            print(traceback.format_exc())
            
            # Envoyer un message d'erreur au topic d'erreurs
            for msg in messages:
                await producer.send_and_wait(
                    KAFKA_TOPIC_ERRORS,
                    json.dumps({
                        "status": "error",
                        "timestamp": datetime.now().isoformat(),
                        "message_id": msg.get("id", "unknown"),
                        "error": error_details
                    }).encode('utf-8')
                )
            
            return False     
        
async def display_stats():
    """Affiche régulièrement les statistiques de performance"""
    global start_time, messages_processed, batches_processed, db_errors, kafka_errors
    
    while True:
        elapsed_time = time.time() - start_time
        messages_per_second = messages_processed / elapsed_time if elapsed_time > 0 else 0
        
        print("\n📊 STATISTIQUES DE PERFORMANCE 📊")
        print(f"Temps d'exécution: {elapsed_time:.2f} secondes")
        print(f"Messages traités: {messages_processed}")
        print(f"Batches traités: {batches_processed}")
        print(f"Erreurs DB: {db_errors}")
        print(f"Erreurs Kafka: {kafka_errors}")
        print(f"Débit: {messages_per_second:.2f} messages/seconde")
        print("=" * 40)
        
        await asyncio.sleep(30)  # Afficher les stats toutes les 30 secondes

async def consume_from_kafka(batch_size: int = 10, max_wait_time: float = 5.0):
    """Consomme les messages de Kafka en batch de manière asynchrone"""
    global kafka_errors
    
    # Créer un pool de connexions à la base de données
    db_pool = await create_db_pool()
    
    # Démarrer la tâche de monitoring
    monitoring_task = asyncio.create_task(display_stats())
    
    # Déclarer les ressources en dehors du try/except pour pouvoir les fermer dans finally
    consumer = None
    producer = None
    
    try:
        # Configuration du consumer Kafka asynchrone
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC_INPUT,
            bootstrap_servers=KAFKA_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Configuration du producer Kafka asynchrone
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_SERVERS
        )
        
        # Démarrer le consumer et le producer
        await consumer.start()
        await producer.start()
        
        print(f"🚦 Consumer Kafka asynchrone démarré (taille du batch: {batch_size})...")
        
        batch_messages = []
        batch_start_time = time.time()
        
        try:
            while True:
                # Récupérer les messages disponibles
                try:
                    message = await asyncio.wait_for(consumer.getone(), timeout=0.1)
                    batch_messages.append(message)
                    
                    # Si le batch est complet, le traiter
                    if len(batch_messages) >= batch_size:
                        await process_batch(batch_messages, consumer, producer, db_pool)
                        batch_messages = []
                        batch_start_time = time.time()
                        
                except asyncio.TimeoutError:
                    # Aucun message disponible, vérifier si nous devons traiter le batch actuel
                    current_time = time.time()
                    if batch_messages and (current_time - batch_start_time) >= max_wait_time:
                        await process_batch(batch_messages, consumer, producer, db_pool)
                        batch_messages = []
                        batch_start_time = current_time
                    
                    # Courte pause pour éviter une utilisation intensive du CPU
                    await asyncio.sleep(0.01)
                
        except asyncio.CancelledError:
            print("Tâche de consommation annulée")
        finally:
            # Traiter les messages restants avant de fermer
            if batch_messages:
                await process_batch(batch_messages, consumer, producer, db_pool)
            
    except Exception as e:
        kafka_errors += 1
        print(f"❌ Erreur critique du consumer: {str(e)}")
        print(traceback.format_exc())
    finally:
        # Annuler la tâche de monitoring
        monitoring_task.cancel()
        try:
            await monitoring_task
        except asyncio.CancelledError:
            pass
        
        # Fermer les connexions Kafka
        if consumer is not None:
            await consumer.stop()
        
        if producer is not None:
            await producer.stop()
        
        # Fermer le pool de connexions
        await db_pool.close()
        print("🛑 Fermeture des connexions")

async def process_batch(
    batch_messages: List, 
    consumer: AIOKafkaConsumer, 
    producer: AIOKafkaProducer, 
    db_pool: asyncpg.Pool
):
    """Traite un batch de messages et gère les offsets"""
    if not batch_messages:
        return
        
    print(f"📥 Traitement d'un batch de {len(batch_messages)} messages")
    
    # Extraire les valeurs des messages
    messages_values = [msg.value for msg in batch_messages]
    
    # Enregistrer dans PostgreSQL
    success = await save_to_postgres_batch(messages_values, producer, db_pool)
    
    # Committer les offsets si le traitement a réussi
    if success:
        # Créer un dictionnaire partition -> dernier offset
        offsets = {}
        for msg in batch_messages:
            tp = msg.topic, msg.partition
            if tp not in offsets or offsets[tp] < msg.offset:
                offsets[tp] = msg.offset
        
        # Committer chaque offset
        for (topic, partition), offset in offsets.items():
            await consumer.commit({
                TopicPartition(topic, partition): 
                OffsetAndMetadata(offset + 1, "")
            })
        print(f"✓ Offsets commités pour {len(offsets)} partitions")
    else:
        print("⚠️ Échec du traitement batch - offsets non commités")

async def main():
    """Fonction principale"""
    # Paramètres configurables
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
    MAX_WAIT_TIME = float(os.getenv("MAX_WAIT_TIME", "5.0"))  # secondes
    
    try:
        print(f"🚀 Démarrage du consumer de trafic asynchrone à {datetime.now().strftime('%H:%M:%S')}")
        await consume_from_kafka(BATCH_SIZE, MAX_WAIT_TIME)
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du programme par l'utilisateur")
    except Exception as e:
        print(f"❌ Erreur fatale: {str(e)}")
        print(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main())