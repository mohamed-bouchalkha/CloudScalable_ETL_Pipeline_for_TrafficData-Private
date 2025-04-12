import requests
import json
from datetime import datetime

def get_road_data():
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # Requête améliorée pour Rabat
    query = """
    [out:json];
    // Définir la zone de Rabat (utilisant plusieurs noms possibles)
    area["name"="Rabat"]->.rabat;
    area["name:fr"="Rabat"]->.rabat2;
    area["name:ar"="الرباط"]->.rabat3;
    
    // Combiner les zones
    (.rabat; .rabat2; .rabat3;)->.searchArea;
    
    // Récupérer les routes principales dans la zone
    (
        way["highway"="motorway"](area.searchArea);
        way["highway"="trunk"](area.searchArea);
        way["highway"="primary"](area.searchArea);
        way["highway"="secondary"](area.searchArea);
        way["highway"="tertiary"](area.searchArea);
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
            "city": "Rabat",
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
    
    # Identifier les intersections
    node_count = {}
    for road in organized_data["roads"]:
        for node_id in road.get("node_ids", []):
            if node_id in node_count:
                node_count[node_id] += 1
            else:
                node_count[node_id] = 1
    
    # Marquer les nœuds qui sont des intersections
    intersection_nodes = [node_id for node_id, count in node_count.items() if count > 1]
    organized_data["metadata"]["intersections_count"] = len(intersection_nodes)
    organized_data["intersections"] = intersection_nodes
    
    # Sauvegarder dans un fichier JSON
    try:
        with open('rabat_roads.json', 'w', encoding='utf-8') as f:
            json.dump(organized_data, f, indent=2, ensure_ascii=False)
        print(f"Données sauvegardées dans rabat_roads.json")
        print(f"Nombre de routes trouvées : {len(organized_data['roads'])}")
        print(f"Nombre d'intersections identifiées : {len(intersection_nodes)}")
        
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