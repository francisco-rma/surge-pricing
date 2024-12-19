import json
import logging
from typing import Dict

import branca
import dash
import folium
from dash import dcc, html

from app.data_aggregator_service import REDIS_CLIENT
from app.driver_position.service import DriverPositionAggregator
from app.orders.service import OrderAggregator
from app.surge_pricing.service import SurgePricingCalculator


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
    driver_position_aggregator = DriverPositionAggregator(REDIS_CLIENT)
    driver_count_data = driver_position_aggregator.get_driver_count_for_all_cells(
        cell_resolution=cell_resolution
    )
    return {
        data.region: data.count for data in driver_count_data.driver_position_counts
    }


def get_order_count_dict(cell_resolution: int = 7) -> Dict[str, int]:
    """Fetch order count data and return it as a dictionary."""
    order_aggregator = OrderAggregator(
        REDIS_CLIENT
    )  # Assuming OrderAggregator is defined
    order_count_data = order_aggregator.get_order_count_for_all_cells(cell_resolution)
    return {data.region: data.count for data in order_count_data.driver_position_counts}


def get_surge_price_dict(cell_resolution: int = 7) -> Dict[str, float]:
    """Fetch surge price data and return it as a dictionary."""
    # Implement the logic to fetch surge price data
    driver_position_aggregator = DriverPositionAggregator(
        redis_client=REDIS_CLIENT, time_window_minutes=1
    )
    order_aggregator = OrderAggregator(redis_client=REDIS_CLIENT, time_window_minutes=1)

    surge_price_calculator = SurgePricingCalculator(
        base_price=1,
        driver_position_aggregator=driver_position_aggregator,
        order_aggregator=order_aggregator,
    )
    surge_price_data = surge_price_calculator.calculate_surge_for_all_cells(
        cell_resolution
    )

    return surge_price_data


def update_geojson_with_driver_counts(
    geojson_data: dict, driver_count_dict: Dict[str, int]
) -> dict:
    """Update GeoJSON features with driver count data."""
    for feature in geojson_data.get("features", []):
        h3_index = feature["properties"].get("h3_index")
        feature["properties"]["driver_count"] = driver_count_dict.get(h3_index, 0)

    return geojson_data


def update_geojson_with_order_counts(
    geojson_data: dict, order_count_dict: Dict[str, int]
) -> dict:
    """Update GeoJSON features with order count data."""
    for feature in geojson_data.get("features", []):
        h3_index = feature["properties"].get("h3_index")
        feature["properties"]["order_count"] = order_count_dict.get(h3_index, 0)

    return geojson_data


def update_geojson_with_surge_prices(
    geojson_data: dict, surge_price_dict: Dict[str, float]
) -> dict:
    """Update GeoJSON features with surge price data."""
    for feature in geojson_data.get("features", []):
        h3_index = feature["properties"].get("h3_index")
        feature["properties"]["surge_price"] = surge_price_dict.get(h3_index, 0)

    return geojson_data


def get_geojson_layer(cell_resolution: int = 7) -> dict:
    """Generate the GeoJSON layer with driver counts."""
    try:
        geojson_data = load_geojson_from_file("app/geojson_h3/resolution_7.geojson")

        driver_count_dict = get_driver_count_dict(cell_resolution)
        geojson_data = update_geojson_with_driver_counts(
            geojson_data, driver_count_dict
        )

        return geojson_data

    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        return {}


def generate_folium_map_driver_count():
    colormap = branca.colormap.linear.YlGnBu_09.scale(0, 100)

    def style_function(feature):
        driver_count = feature["properties"].get("driver_count", 0)

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


def generate_folium_map_order_count():
    """Generate map with order counts."""
    colormap = branca.colormap.linear.YlOrRd_09.scale(0, 3000)

    def style_function(feature):
        order_count = feature["properties"].get("order_count", 0)
        return {
            "fillColor": colormap(order_count),
            "color": "black",
            "weight": 1,
            "fillOpacity": 0.6,
        }

    folium_map = folium.Map(location=[-19.9191, -43.9378], zoom_start=13)
    folium.TileLayer("cartodbpositron").add_to(folium_map)
    geojson_data = get_geojson_layer(cell_resolution=7)

    order_count_dict = get_order_count_dict(cell_resolution=7)
    geojson_data = update_geojson_with_order_counts(geojson_data, order_count_dict)

    geojson_layer = folium.GeoJson(
        geojson_data,
        name="H3 Cells",
        tooltip=folium.GeoJsonTooltip(
            fields=["order_count"], aliases=["Order Position Count:"], sticky=True
        ),
        style_function=style_function,
    ).add_to(folium_map)

    return folium_map._repr_html_()


def generate_folium_map_surge_price():
    """Generate map with surge prices."""
    colormap = branca.colormap.linear.YlOrRd_09.scale(0, 5)  # Adjust scale as needed

    def style_function(feature):
        surge_price = feature["properties"].get("surge_price", 0)
        return {
            "fillColor": colormap(surge_price),
            "color": "black",
            "weight": 1,
            "fillOpacity": 0.6,
        }

    folium_map = folium.Map(location=[-19.9191, -43.9378], zoom_start=13)
    folium.TileLayer("cartodbpositron").add_to(folium_map)

    geojson_data = get_geojson_layer(cell_resolution=7)
    surge_price_dict = get_surge_price_dict(cell_resolution=7)
    geojson_data = update_geojson_with_surge_prices(geojson_data, surge_price_dict)

    geojson_layer = folium.GeoJson(
        geojson_data,
        name="H3 Cells with Surge Price",
        tooltip=folium.GeoJsonTooltip(
            fields=["surge_price"], aliases=["Surge Price:"], sticky=True
        ),
        style_function=style_function,
    ).add_to(folium_map)

    return folium_map._repr_html_()


app_dash = dash.Dash(__name__, requests_pathname_prefix="/dash/")
app_dash.layout = html.Div(
    [
        html.H1(
            "Driver and Order Position Counts", style={"textAlign": "center"}
        ),  # Main title
        html.Div(
            [
                # First map - Driver count map
                html.Div(
                    [
                        html.Iframe(
                            id="driver-count-map",
                            srcDoc=generate_folium_map_driver_count(),
                            width="100%",
                            height="800px",
                            style={"border": "none", "margin": "0", "display": "block"},
                        )
                    ],
                    style={
                        "width": "50%",
                        "display": "inline-block",
                        "marginBottom": "5px",
                    },  # Set width to 50% for side-by-side
                ),
                # Second map - Order count map
                html.Div(
                    [
                        html.Iframe(
                            id="order-count-map",
                            srcDoc=generate_folium_map_order_count(),
                            width="100%",
                            height="800px",
                            style={"border": "none", "margin": "0", "display": "block"},
                        )
                    ],
                    style={
                        "width": "50%",
                        "display": "inline-block",
                        "marginBottom": "5px",
                    },  # Set width to 50% for side-by-side
                ),
            ],
            style={
                "textAlign": "center",
                "margin": "0 auto",
            },
        ),
        html.Div(
            [
                html.H2(
                    "Surge Price", style={"textAlign": "center", "marginTop": "20px"}
                ),  # Title above the surge price map
                html.Div(
                    [
                        html.Iframe(
                            id="surge-price-map",
                            srcDoc=generate_folium_map_surge_price(),
                            width="100%",
                            height="800px",
                            style={"border": "none", "margin": "0", "display": "block"},
                        )
                    ],
                    style={"width": "100%", "display": "inline-block"},
                ),
            ],
            style={"marginTop": "10px"},  # Reduced margin between maps
        ),
        html.Button(id="map-submit-button", n_clicks=0, children="Submit"),
        dcc.Store(id="map-refresh-store", data="initial"),
    ]
)


@app_dash.callback(
    dash.dependencies.Output("driver-count-map", "srcDoc"),
    [dash.dependencies.Input("map-submit-button", "n_clicks")],
)
def update_driver_map(n_clicks):
    """Callback to update the driver count map when the button is clicked."""
    if n_clicks > 0:
        return generate_folium_map_driver_count()
    else:
        return dash.no_update


@app_dash.callback(
    dash.dependencies.Output("order-count-map", "srcDoc"),
    [dash.dependencies.Input("map-submit-button", "n_clicks")],
)
def update_order_map(n_clicks):
    """Callback to update the order count map when the button is clicked."""
    if n_clicks > 0:
        return generate_folium_map_order_count()
    else:
        return dash.no_update


@app_dash.callback(
    dash.dependencies.Output("surge-price-map", "srcDoc"),
    [dash.dependencies.Input("map-submit-button", "n_clicks")],
)
def update_surge_price_map(n_clicks):
    """Callback to update the surge price map when the button is clicked."""
    if n_clicks > 0:
        return generate_folium_map_surge_price()
    else:
        return dash.no_update
