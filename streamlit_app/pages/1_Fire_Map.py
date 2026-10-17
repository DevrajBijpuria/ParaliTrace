import datetime as dt
import folium
import numpy as np
import streamlit as st
import streamlit.components.v1 as components
from folium.plugins import HeatMap

st.set_page_config(page_title="Fire Map", page_icon="🔥", layout="wide")
conn = st.connection("snowflake")
DELHI_LON, DELHI_LAT = 77.22, 28.63

st.title("Fire → wind → Delhi")
st.caption("Pick a day to see where crops were burning, which way the wind blew, and whose smoke reached Delhi.")

c1, c2, c3 = st.columns([1, 1.4, 1.4])
season = c1.selectbox("Season", [2025, 2024, 2023])
peak = conn.query(f"""
    SELECT date_ist, SUM(total_frp_mw) AS frp
    FROM PARALITRACE.GOLD.MART_FIRES_DAILY_BY_REGION
    WHERE season = {season} AND region_key IN ('punjab', 'haryana')
    GROUP BY date_ist
    ORDER BY frp DESC
    LIMIT 1
""", ttl=3600)
day = c2.date_input("Day (IST)", value=peak["DATE_IST"].iloc[0],
                    min_value=dt.date(season, 9, 15), max_value=dt.date(season, 12, 15))
sats = {
    "MODIS Terra (late morning)": "MODIS_Terra_CorrectedReflectance_TrueColor",
    "MODIS Aqua (early afternoon)": "MODIS_Aqua_CorrectedReflectance_TrueColor",
    "VIIRS Suomi NPP (early afternoon)": "VIIRS_SNPP_CorrectedReflectance_TrueColor",
}
sat = c3.selectbox("Satellite photo", list(sats), help="If the image has a black gap, try another satellite.")

fires = conn.query(f"""
    SELECT cell_lat AS lat, cell_lon AS lon, frp_mw, region_key, n_satellites,
           TO_CHAR(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hour_start_utc), 'HH24:MI') AS time_ist
    FROM PARALITRACE.SILVER.FCT_FIRE_DETECTION
    WHERE DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hour_start_utc)) = '{day}'
      AND NOT is_low_confidence
      AND region_key IN ('punjab', 'haryana')
""", ttl=3600)

wind = conn.query(f"""
    SELECT r.point_key, r.point_name, r.lat, r.lon,
           AVG(w.wind_u_100m) AS u, AVG(w.wind_v_100m) AS v, AVG(w.wind_speed_100m) AS ws
    FROM PARALITRACE.SILVER.FCT_WEATHER_HOURLY w
    JOIN PARALITRACE.SILVER.DIM_ROUTE_POINT r ON r.point_key = w.point_key
    WHERE DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', w.hour_start_utc)) = '{day}'
    GROUP BY r.point_key, r.point_name, r.lat, r.lon
""", ttl=3600)

stations = conn.query(f"""
    SELECT s.station_name, s.lat, s.lon, AVG(a.pm25) AS pm25
    FROM PARALITRACE.SILVER.FCT_AQ_HOURLY a
    JOIN PARALITRACE.SILVER.DIM_STATION s ON s.station_key = a.station_key
    WHERE a.is_usable
      AND DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.hour_start_utc)) = '{day}'
    GROUP BY s.station_name, s.lat, s.lon
""", ttl=3600)

trails = conn.query(f"""
    SELECT ROUND(f.cell_lat * 2) / 2 AS lat, ROUND(f.cell_lon * 2) / 2 AS lon,
           COUNT(*) AS fires, SUM(a.smoke_weight) AS smoke, AVG(a.travel_hours) AS travel_h
    FROM PARALITRACE.GOLD.FCT_SMOKE_ARRIVALS a
    JOIN PARALITRACE.SILVER.FCT_FIRE_DETECTION f ON f.fire_id = a.fire_id
    WHERE DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.arrival_hour_utc)) = '{day}'
    GROUP BY 1, 2
""", ttl=3600)

# ---------- plain-English summary ----------
total_frp = fires["FRP_MW"].sum()
share_of_peak = total_frp / peak["FRP"].iloc[0] if peak["FRP"].iloc[0] else 0
level = ("one of the worst fire days" if share_of_peak > 0.6 else
         "a heavy fire day" if share_of_peak > 0.25 else
         "a moderate fire day" if share_of_peak > 0.05 else "a quiet day")

lines = [f"On **{day:%d %B %Y}**, satellites spotted **{len(fires):,} farm fires** in Punjab and Haryana, "
         f"{level} of the {season} season."]

up = wind[wind["POINT_KEY"] != "delhi"]
if len(up):
    u, v, ws = up["U"].mean(), up["V"].mean(), up["WS"].mean()
    from_deg = (np.degrees(np.arctan2(-u, -v)) + 360) % 360
    dirs = ["north", "north-east", "east", "south-east", "south", "south-west", "west", "north-west"]
    from_name = dirs[int(((from_deg + 22.5) % 360) // 45)]
    if ws < 2:
        lines.append("The air was **almost still**, so smoke lingered over the region and drifted slowly.")
    elif from_deg >= 270 or from_deg < 15:
        lines.append(f"The wind blew **from the {from_name}** at about {ws:.0f} m/s, **carrying smoke towards Delhi**.")
    else:
        lines.append(f"The wind blew **from the {from_name}** at about {ws:.0f} m/s, "
                     "which mostly **kept the smoke away from Delhi**.")

if len(trails):
    lines.append(f"We estimate smoke from about **{int(trails['FIRES'].sum()):,} fires reached Delhi** on this day. "
                 f"On average that smoke had been travelling for **{trails['TRAVEL_H'].mean():.0f} hours**.")
else:
    lines.append("We estimate **no farm-fire smoke reached Delhi** on this day.")

bins = [30, 60, 90, 120, 250]
aqi_names = ["Good", "Satisfactory", "Moderate", "Poor", "Very poor", "Severe"]
aqi_hex = ["#009950", "#8BC34A", "#FFD600", "#FF8C00", "#E53935", "#8E24AA"]
if len(stations):
    pm = stations["PM25"].mean()
    band = aqi_names[int(np.digitize(pm, bins, right=True))]
    if pm > 60:
        lines.append(f"Delhi's air averaged **{pm:.0f} µg/m³** of PM2.5, rated **{band}**, "
                     f"about **{pm / 60:.1f}× India's safe daily limit**.")
    else:
        lines.append(f"Delhi's air averaged **{pm:.0f} µg/m³** of PM2.5, rated **{band}**, within India's safe daily limit.")

with st.container(border=True):
    st.markdown("#### 🗣️ In plain words")
    st.markdown(" ".join(lines))

m1, m2, m3, m4 = st.columns(4)
m1.metric("Farm fires", f"{len(fires):,}")
m2.metric("Fire power", f"{total_frp:,.0f} MW")
m3.metric("Fires (past 1–3 days) whose smoke reached Delhi", f"{int(trails['FIRES'].sum()) if len(trails) else 0:,}")
m4.metric("Delhi PM2.5", f"{stations['PM25'].mean():,.0f} µg/m³" if len(stations) else "no data")

# ---------- wind arrow geometry ----------
w = wind[np.hypot(wind["U"], wind["V"]) > 0].copy()
mag = np.hypot(w["U"], w["V"])
dx, dy = w["U"] / mag, w["V"] / mag
length = (mag * 0.07).clip(lower=0.12)
hx, hy = w["LON"] + dx * length, w["LAT"] + dy * length
wing, ang = 0.35 * length, np.radians(28)
lx = hx - wing * (dx * np.cos(ang) - dy * np.sin(ang))
ly = hy - wing * (dx * np.sin(ang) + dy * np.cos(ang))
rx = hx - wing * (dx * np.cos(-ang) - dy * np.sin(-ang))
ry = hy - wing * (dx * np.sin(-ang) + dy * np.cos(-ang))
w["path"] = [[[x0, y0], [x1, y1], [x2, y2], [x1, y1], [x3, y3]]
             for x0, y0, x1, y1, x2, y2, x3, y3 in zip(w["LON"], w["LAT"], hx, hy, lx, ly, rx, ry)]

stations["band"] = [aqi_names[i] for i in np.digitize(stations["PM25"], bins, right=True)]
stations["hex"] = [aqi_hex[i] for i in np.digitize(stations["PM25"], bins, right=True)]

# ---------- map ----------
gibs = (f"https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/{sats[sat]}/default/"
        f"{day:%Y-%m-%d}/GoogleMapsCompatible_Level9/{{z}}/{{y}}/{{x}}.jpg")

m = folium.Map(location=[29.9, 75.9], zoom_start=7, tiles=None, control_scale=True)
folium.TileLayer(tiles=gibs, attr="NASA GIBS / Worldview", name="Satellite photo",
                 max_native_zoom=9, max_zoom=12).add_to(m)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/Reference/"
          "World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}",
    attr="Esri", name="Borders and place names", overlay=True).add_to(m)

# fire glow (optional, off by default)
if len(fires):
    HeatMap(fires[["LAT", "LON", "FRP_MW"]].values.tolist(), name="Fire glow", show=False,
            radius=18, blur=22, min_opacity=0.3,
            gradient={0.2: "#7A1F0A", 0.5: "#E06A2E", 0.8: "#F5A03C", 1.0: "#FFDC78"}).add_to(m)

# smoke trails: curved lines from the 25 strongest source areas to Delhi
trail_group = folium.FeatureGroup(name="Smoke trails to Delhi (our model)")
top = trails.nlargest(25, "SMOKE") if len(trails) else trails
for r in top.itertuples():
    x0, y0, x2, y2 = r.LON, r.LAT, DELHI_LON, DELHI_LAT
    mx, my = (x0 + x2) / 2, (y0 + y2) / 2
    cx, cy = mx - (y2 - y0) * 0.18, my + (x2 - x0) * 0.18          # bend the line sideways
    t = np.linspace(0, 1, 24)
    bx = (1 - t) ** 2 * x0 + 2 * (1 - t) * t * cx + t ** 2 * x2      # quadratic curve
    by = (1 - t) ** 2 * y0 + 2 * (1 - t) * t * cy + t ** 2 * y2
    folium.PolyLine(list(zip(by, bx)), color="#FF8A3D", opacity=0.8,
                    weight=1.5 + 5 * r.SMOKE / top["SMOKE"].max(),
                    tooltip=f"{r.FIRES} fires · smoke travelled ~{r.TRAVEL_H:.0f} h to Delhi").add_to(trail_group)
trail_group.add_to(m)

# fires as red dots (Worldview style)
fire_fc = {"type": "FeatureCollection", "features": [
    {"type": "Feature",
     "geometry": {"type": "Point", "coordinates": [r.LON, r.LAT]},
     "properties": {"frp": round(r.FRP_MW, 1), "time": r.TIME_IST}}
    for r in fires.itertuples()]}
folium.GeoJson(
    fire_fc, name="Farm fires",
    marker=folium.CircleMarker(radius=2.5, fill=True, fill_opacity=0.9, weight=0,
                               color="#FF2A1F", fill_color="#FF2A1F"),
    tooltip=folium.GeoJsonTooltip(fields=["frp", "time"], aliases=["Fire power (MW)", "Seen at (IST)"]),
).add_to(m)

wind_group = folium.FeatureGroup(name="Wind")
for r in w.itertuples():
    folium.PolyLine([[y, x] for x, y in r.path], color="#5BB8FF", weight=3, opacity=0.95,
                    tooltip=f"Wind at {r.POINT_NAME}: {r.WS:.1f} m/s").add_to(wind_group)
wind_group.add_to(m)

station_group = folium.FeatureGroup(name="Delhi air stations")
for r in stations.itertuples():
    folium.CircleMarker([r.LAT, r.LON], radius=7, color="#14161A", weight=2, fill=True,
                        fill_color=r.hex, fill_opacity=1,
                        tooltip=f"{r.STATION_NAME}: {r.PM25:.0f} µg/m³ ({r.band})").add_to(station_group)
station_group.add_to(m)

folium.CircleMarker([DELHI_LAT, DELHI_LON], radius=11, color="#FFFFFF", weight=2, fill=False,
                    tooltip=folium.Tooltip("Delhi", permanent=True, direction="right")).add_to(m)

folium.LayerControl(collapsed=True).add_to(m)
components.html(m.get_root().render(), height=650)

st.markdown("""
<div style="display:flex;flex-wrap:wrap;gap:18px;font-size:0.85rem;color:#B9B6B0;margin-top:6px">
  <span><span style="color:#FF2A1F">●</span> Farm fire spotted by satellite</span>
  <span><span style="color:#FF8A3D">━</span> Smoke trail to Delhi (thicker = more smoke)</span>
  <span><span style="color:#5BB8FF">➜</span> Wind (longer = stronger)</span>
  <span>Delhi air:
    <span style="color:#009950">●</span> Good <span style="color:#8BC34A">●</span> Satisfactory
    <span style="color:#FFD600">●</span> Moderate <span style="color:#FF8C00">●</span> Poor
    <span style="color:#E53935">●</span> Very poor <span style="color:#8E24AA">●</span> Severe</span>
</div>
""", unsafe_allow_html=True)
st.caption("The background is a real NASA satellite photo of the day; the grey-white haze is smoke and smog. "
           "The orange trails are our model's estimate of which fire areas sent smoke to Delhi on this day, "
           "usually from fires 1–2 days earlier. Use the layer button (top right) to switch layers on or off.")