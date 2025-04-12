import requests
import json
from datetime import datetime

def get_road_segments(city):
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    query = f"""
    [out:json];
    area["name"="{city}"]->.city;
    (
        way["highway"="motorway"](area.city);
        way["highway"="trunk"](area.city);
        way["highway"="primary"](area.city);
        way["highway"="secondary"](area.city);
        way["highway"="tertiary"](area.city);
        way["route"="road"](area.city);
    );
    out body geom;
    """
    
    print(f"Récupération des segments de route pour {city}...")
    response = requests.get(overpass_url, params={"data": query})
    
    if response.status_code != 200:
        print(f"Erreur lors de la requête pour {city}: {response.status_code}")
        return
    
    raw_data = response.json()
    road_segments = []
    
    for way in raw_data.get('elements', []):
        if way.get('type') == 'way':
            road_segment = {
                "id": way.get('id'),
                "type": way.get('type'),
                "city": city,
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
                }
            }
            road_segments.append(road_segment)
    
    output_data = {
        "metadata": {
            "city": city,
            "date_extracted": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_segments": len(road_segments),
            "api_version": raw_data.get('version'),
            "generator": raw_data.get('generator'),
            "osm_timestamp": raw_data.get('osm3s', {}).get('timestamp_osm_base'),
            "copyright": raw_data.get('osm3s', {}).get('copyright')
        },
        "road_segments": road_segments
    }
    
    filename = f"{city.lower().replace(' ', '_')}_road_segments.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        print(f"Données sauvegardées dans {filename}")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde pour {city}: {e}")

if __name__ == "__main__":
    cities = [
        "Paris", "Lyon", "Casablanca", "Marrakech", "Cairo", "Agadir", "Fès", "Chicago"
    ]
    for city in cities:
        get_road_segments(city)
