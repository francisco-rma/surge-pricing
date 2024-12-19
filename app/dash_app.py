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


class GeoJSONUpdater:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.geojson_data = self._load_geojson()

    def _load_geojson(self) -> dict:
        try:
            with open(self.filepath, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"GeoJSON file not found at {self.filepath}")
        except json.JSONDecodeError:
            raise ValueError(f"Error decoding JSON in the file at {self.filepath}")

    def update_features(self, update_dict: Dict[str, any], key: str):
        for feature in self.geojson_data.get("features", []):
            h3_index = feature["properties"].get("h3_index")
            feature["properties"][key] = update_dict.get(h3_index, 0)

        return self.geojson_data


class MapGenerator:
    def __init__(
        self, center: list, zoom_start: int, colormap_scale: tuple, colormap_name: str
    ):
        self.map = folium.Map(location=center, zoom_start=zoom_start)
        self.colormap = branca.colormap.linear.YlGnBu_09.scale(*colormap_scale)

    def add_geojson_layer(self, geojson_data: dict, key: str, tooltip_alias: str):
        def style_function(feature):
            value = feature["properties"].get(key, 0)
            return {
                "fillColor": self.colormap(value),
                "color": "black",
                "weight": 1,
                "fillOpacity": 0.6,
            }

        folium.TileLayer("cartodbpositron").add_to(self.map)

        folium.GeoJson(
            geojson_data,
            name="H3 Cells",
            tooltip=folium.GeoJsonTooltip(
                fields=[key],
                aliases=[tooltip_alias],
                localize=True,
                sticky=True,
                labels=True,
            ),
            style_function=style_function,
        ).add_to(self.map)

        return self.map._repr_html_()


class DataProvider:
    @staticmethod
    def get_driver_count_dict(cell_resolution: int = 7) -> Dict[str, int]:
        driver_position_aggregator = DriverPositionAggregator(REDIS_CLIENT)
        driver_count_data = driver_position_aggregator.get_driver_count_for_all_cells(
            cell_resolution=cell_resolution
        )
        return {
            data.region: data.count for data in driver_count_data.driver_position_counts
        }

    @staticmethod
    def get_order_count_dict(cell_resolution: int = 7) -> Dict[str, int]:
        order_aggregator = OrderAggregator(REDIS_CLIENT)
        order_count_data = order_aggregator.get_order_count_for_all_cells(
            cell_resolution
        )
        return {
            data.region: data.count for data in order_count_data.driver_position_counts
        }

    @staticmethod
    def get_surge_price_dict(cell_resolution: int = 7) -> Dict[str, float]:
        driver_position_aggregator = DriverPositionAggregator(
            redis_client=REDIS_CLIENT, time_window_minutes=1
        )
        order_aggregator = OrderAggregator(
            redis_client=REDIS_CLIENT, time_window_minutes=1
        )

        surge_price_calculator = SurgePricingCalculator(
            base_price=1,
            driver_position_aggregator=driver_position_aggregator,
            order_aggregator=order_aggregator,
        )
        surge_price_data = surge_price_calculator.calculate_surge_for_all_cells(
            cell_resolution
        )

        return surge_price_data


# Usage


# Generate Driver Count Map
def generate_driver_count_map():
    updater = GeoJSONUpdater("app/geojson_h3/resolution_7.geojson")
    driver_count_dict = DataProvider.get_driver_count_dict(cell_resolution=7)
    geojson_data = updater.update_features(driver_count_dict, key="driver_count")

    map_generator = MapGenerator(
        center=[-19.9191, -43.9378],
        zoom_start=13,
        colormap_scale=(0, 200),
        colormap_name="YlGnBu_09",
    )
    return map_generator.add_geojson_layer(
        geojson_data, key="driver_count", tooltip_alias="Driver Count:"
    )


# Generate Order Count Map
def generate_order_count_map():
    updater = GeoJSONUpdater("app/geojson_h3/resolution_7.geojson")
    order_count_dict = DataProvider.get_order_count_dict(cell_resolution=7)
    geojson_data = updater.update_features(order_count_dict, key="order_count")

    map_generator = MapGenerator(
        center=[-19.9191, -43.9378],
        zoom_start=13,
        colormap_scale=(0, 3000),
        colormap_name="YlOrRd_09",
    )
    return map_generator.add_geojson_layer(
        geojson_data, key="order_count", tooltip_alias="Order Count:"
    )


# Generate Surge Price Map
def generate_surge_price_map():
    updater = GeoJSONUpdater("app/geojson_h3/resolution_7.geojson")
    surge_price_dict = DataProvider.get_surge_price_dict(cell_resolution=7)
    geojson_data = updater.update_features(surge_price_dict, key="surge_price")

    map_generator = MapGenerator(
        center=[-19.9191, -43.9378],
        zoom_start=13,
        colormap_scale=(0, 5),
        colormap_name="YlOrRd_09",
    )
    return map_generator.add_geojson_layer(
        geojson_data, key="surge_price", tooltip_alias="Surge Price:"
    )


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
                            srcDoc=generate_driver_count_map(),
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
                            srcDoc=generate_order_count_map(),
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
                            srcDoc=generate_surge_price_map(),
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
        return generate_driver_count_map()
    else:
        return dash.no_update


@app_dash.callback(
    dash.dependencies.Output("order-count-map", "srcDoc"),
    [dash.dependencies.Input("map-submit-button", "n_clicks")],
)
def update_order_map(n_clicks):
    """Callback to update the order count map when the button is clicked."""
    if n_clicks > 0:
        return generate_order_count_map()
    else:
        return dash.no_update


@app_dash.callback(
    dash.dependencies.Output("surge-price-map", "srcDoc"),
    [dash.dependencies.Input("map-submit-button", "n_clicks")],
)
def update_surge_price_map(n_clicks):
    """Callback to update the surge price map when the button is clicked."""
    if n_clicks > 0:
        return generate_surge_price_map()
    else:
        return dash.no_update
