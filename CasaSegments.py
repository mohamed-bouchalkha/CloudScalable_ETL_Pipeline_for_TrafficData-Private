import requests
import json
from datetime import datetime

def get_road_data():
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # Requête améliorée pour Casablanca
    query = """
    [out:json];
    // Définir la zone de Casablanca (utilisant plusieurs noms possibles)
    area["name"="Casablanca"]->.casablanca;
    area["name:fr"="Casablanca"]->.casablanca2;
    area["name:ar"="الدار البيضاء"]->.casablanca3;
    
    // Combiner les zones
    (.casablanca; .casablanca2; .casablanca3;)->.searchArea;
    
    // Récupérer toutes les routes dans la zone
    (
        way["highway"](area.searchArea);
        way["route"="road"](area.searchArea);
    );
    
    // Inclure la géométrie et les tags
    out geom;
    """
    
    print("Récupération des données en cours...")
    response = requests.get(overpass_url, params={"data": query})
    
    if response.status_code != 200:
        print(f"Erreur lors de la requête: {response.status_code}")
        return
    
    raw_data = response.json()
    
    organized_data = {
        "metadata": {
            "city": "Casablanca",
            "date_extracted": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "api_version": raw_data.get('version'),
            "generator": raw_data.get('generator'),
            "osm_timestamp": raw_data.get('osm3s', {}).get('timestamp_osm_base'),
            "copyright": raw_data.get('osm3s', {}).get('copyright')
        },
        "roads": []
    }
    
    # Organiser les données de chaque route
    for way in raw_data.get('elements', []):
        if way.get('type') == 'way' and 'tags' in way:
            road = {
                "id": way.get('id'),
                "type": way.get('type'),
                "location": {
                    "bounds": way.get('bounds'),
                    "coordinates": way.get('geometry')
                },
                "properties": {
                    "name": way['tags'].get('name', 'Non nommé'),
                    "name:fr": way['tags'].get('name:fr', ''),
                    "name:ar": way['tags'].get('name:ar', ''),
                    "highway_type": way['tags'].get('highway', ''),
                    "maxspeed": way['tags'].get('maxspeed', ''),
                    "lanes": way['tags'].get('lanes', ''),
                    "oneway": way['tags'].get('oneway', ''),
                    **way.get('tags', {})
                },
                "node_ids": way.get('nodes', [])
            }
            organized_data["roads"].append(road)
    
    # Sauvegarder dans un fichier JSON
    try:
        with open('casablanca_roads.json', 'w', encoding='utf-8') as f:
            json.dump(organized_data, f, indent=2, ensure_ascii=False)
        print(f"Données sauvegardées dans casablanca_roads.json")
        print(f"Nombre de routes trouvées : {len(organized_data['roads'])}")
        
        # Afficher quelques statistiques
        highway_types = {}
        for road in organized_data["roads"]:
            highway_type = road["properties"]["highway_type"]
            highway_types[highway_type] = highway_types.get(highway_type, 0) + 1
        
        print("\nTypes de routes trouvées :")
        for highway_type, count in highway_types.items():
            print(f"- {highway_type}: {count}")
            
    except Exception as e:
        print(f"Erreur lors de la sauvegarde: {e}")

# Exécuter le script
if __name__ == "__main__":
    get_road_data()