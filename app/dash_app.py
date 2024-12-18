import json
import logging
from typing import Dict

import branca
import dash
import folium
from dash import dcc, html
from dash.dependencies import Input, Output

from app.driver_position.service import \
    get_real_time_driver_count_for_all_cells


def load_geojson_from_file(filepath: str) -> dict:
    """Load GeoJSON data from a file."""
    try:
        with open(filepath, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"GeoJSON file not found at {filepath}")
    except json.JSONDecodeError:
        raise ValueError(f"Error decoding JSON in the file at {filepath}")


def get_driver_count_dict(cell_resolution: int = 7) -> Dict[str, int]:
    """Fetch driver count data and return it as a dictionary."""
    driver_count_data = get_real_time_driver_count_for_all_cells(
        cell_resolution=cell_resolution
    )

    # Creating the dictionary directly from the driver_position_counts
    return {
        data.region: data.count for data in driver_count_data.driver_position_counts
    }


def update_geojson_with_driver_counts(
    geojson_data: dict, driver_count_dict: Dict[str, int]
) -> dict:
    """Update GeoJSON features with driver count data."""
    for feature in geojson_data.get("features", []):
        h3_index = feature["properties"].get("h3_index")
        feature["properties"]["driver_count"] = driver_count_dict.get(h3_index, 0)

    return geojson_data


def get_geojson_layer(cell_resolution: int = 7) -> dict:
    """Generate the GeoJSON layer with driver counts."""
    try:
        # Load the GeoJSON data
        geojson_data = load_geojson_from_file("app/geojson_h3/resolution_7.geojson")

        # Fetch driver count data and prepare it in dictionary format
        driver_count_dict = get_driver_count_dict(cell_resolution)
        # Update GeoJSON data with driver count
        geojson_data = update_geojson_with_driver_counts(
            geojson_data, driver_count_dict
        )

        return geojson_data

    except (FileNotFoundError, ValueError) as e:
        # Handle errors (e.g., GeoJSON file not found or malformed JSON)
        print(f"Error: {e}")
        return {}


def generate_folium_map():
    # Define colormap before using it
    colormap = branca.colormap.linear.YlGnBu_09.scale(0, 3000)

    def style_function(feature):
        # Get the driver count from the feature's properties
        driver_count = feature["properties"].get("driver_count", 0)

        # Use the colormap to determine the color based on driver count
        return {
            "fillColor": colormap(driver_count),  # Set color based on driver count
            "color": "black",  # Border color of the cell
            "weight": 1,  # Border weight
            "fillOpacity": 0.6,  # Transparency of the fill color
        }

    # Set the initial center of the map and zoom level
    folium_map = folium.Map(location=[-19.9191, -43.9378], zoom_start=13)

    # Add a tile layer
    folium.TileLayer("cartodbpositron").add_to(folium_map)

    # Add GeoJSON layer with H3 cells (resolution 7)
    geojson_data = get_geojson_layer(cell_resolution=7)

    # Add GeoJSON data with styling and tooltips
    geojson_layer = folium.GeoJson(
        geojson_data,
        name="H3 Cells",
        tooltip=folium.GeoJsonTooltip(
            fields=["driver_count"],  # The H3 cell ID
            aliases=["Driver Position Count:"],
            localize=True,
            sticky=True,
            labels=True,
        ),
        style_function=style_function,
    ).add_to(folium_map)

    return folium_map._repr_html_()


app_dash = dash.Dash(__name__, requests_pathname_prefix="/dash/")
app_dash.layout = html.Div(
    [
        html.H1("Driver Position Count", style={"textAlign": "center"}),
        # Embed the Folium map using Iframe
        html.Div(
            [
                html.Iframe(
                    id="folium-map",
                    srcDoc=generate_folium_map(),
                    width="80%",  # Set width to 80% (or any value you prefer)
                    height="800px",
                    style={
                        "border": "none",
                        "margin": "0 auto",  # Center the iframe
                        "display": "block",  # Make the iframe block-level
                    },
                )
            ],
            style={
                "width": "70%",  # Adjust the width of the container
                "margin": "0 auto",  # Center the container
                "overflow": "hidden",
            },
        ),
        html.Button(id="map-submit-button", n_clicks=0, children="Submit"),
        dcc.Store(id="map-refresh-store", data="initial"),
    ]
)


@app_dash.callback(
    dash.dependencies.Output("folium-map", "srcDoc"),  # Corrected ID reference
    [dash.dependencies.Input("map-submit-button", "n_clicks")],
)
def update_map(n_clicks):
    """Callback to update the map when the button is clicked."""
    if n_clicks > 0:  # Trigger map update on button click
        return generate_folium_map()
    else:
        return dash.no_update
