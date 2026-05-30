#hwehashdasdadad
"""
Earthquake Early Warning System (EWS) V4
====================================================
SEMUA CODE MILIK ALLAH SWT
"""

import asyncio, json, threading, time, collections, math, os
import numpy as np
from obspy.clients.seedlink.easyseedlink import EasySeedLinkClient
from obspy.signal.trigger import classic_sta_lta, trigger_onset
from obspy.taup import TauPyModel

import websockets


try:
    from quakemigrate.core.lib import recursive_sta_lta as _qm_stalta
    _QM_AVAILABLE = True
    print("[ONSET] QuakeMigrate C library loaded")
except ImportError:
    _QM_AVAILABLE = False
    print("[ONSET] QuakeMigrate tidak ditemukan, pakai obspy classic_sta_lta")

print("Loading TauP model...")
TAUP_MODEL = TauPyModel(model="iasp91")
print("TauP OK")


# ═══════════════════════════════════════════════════════════════════════════════
# KONFIGURASI STASIUN
# ═══════════════════════════════════════════════════════════════════════════════

STATIONS = [
  
    {"net":"GE","sta":"BBJI","cha":"BHZ","lat":-7.46, "lon":107.65,"label":"Garut",
     "thr_on":4.2,"thr_off":0.5,"thr_single":100.0,
     "sta_sec":0.5,"lta_sec":12.0,"java":True},

    {"net":"GE","sta":"UGM", "cha":"SHZ","lat":-7.91, "lon":110.52,"label":"WanaGAMA",
     "thr_on":4.5,"thr_off":0.6,"thr_single":5.5,
     "sta_sec":0.5,"lta_sec":12.0,"java":True},

    # JAGI (Banyuwangi)
    {"net":"GE","sta":"JAGI","cha":"BHZ","lat":-8.47, "lon":114.15,"label":"Banyuwangi",
     "thr_on":5.8,"thr_off":0.5,"thr_single":80.0,
     "sta_sec":0.5,"lta_sec":12.0,"java":True},

    # SMRI (Semarang)
    {"net":"GE","sta":"SMRI","cha":"BHZ","lat":-7.05, "lon":110.44,"label":"Semarang",
     "thr_on":7.5,"thr_off":0.9,"thr_single":30.0,
     "sta_sec":0.5,"lta_sec":12.0,"java":True},

    #  Sumatera 
    {"net":"GE","sta":"LHMI","cha":"BHZ","lat": 5.23, "lon":96.95, "label":"Lhokseumawe",
     "thr_on":8.0,"thr_off":0.7,"sta_sec":0.5,"lta_sec":12.0},
    {"net":"GE","sta":"MNAI","cha":"BHZ","lat":-4.36, "lon":102.96,"label":"Bengkulu",
     "thr_on":7.8,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0},
    {"net":"GE","sta":"GSI", "cha":"BHZ","lat": 1.3,  "lon":97.58, "label":"NIAS",
     "thr_on":7.8,"thr_off":0.7,"sta_sec":0.5,"lta_sec":12.0},

    # Sulawesi 
    {"net":"GE","sta":"TOLI2","cha":"BHZ","lat": 1.11, "lon":120.78,"label":"Toli-Toli",
     "thr_on":7.0,"thr_off":0.7,"sta_sec":0.5,"lta_sec":12.0},
    {"net":"GE","sta":"LUWI","cha":"BHZ", "lat":-1.04, "lon":122.77,"label":"Luwuk",
     "thr_on":7.8,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0},

    # Nusa Tenggara 
    {"net":"GE","sta":"SOEI","cha":"BHZ","lat":-9.76, "lon":124.27,"label":"Soe NTT",
     "thr_on":5.5,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0},
    {"net":"GE","sta":"MMRI","cha":"BHZ","lat":-8.64, "lon":122.24,"label":"Maumere",
     "thr_on":5.8,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0},
    {"net":"GE","sta":"PLAI","cha":"BHZ","lat":-8.83, "lon":117.78,"label":"Sumbawa",
     "thr_on":5.5,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0},

    # Maluku & sekitar 
    {"net":"GE","sta":"TNTI","cha":"BHZ","lat": 0.77, "lon":127.37,"label":"Ternate",
     "thr_on":7.0,"thr_off":0.7,"sta_sec":0.5,"lta_sec":12.0},
    {"net":"GE","sta":"SANI","cha":"BHZ","lat":-2.05, "lon":125.99,"label":"Sanana",
     "thr_on":7.5,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0},
    {"net":"GE","sta":"SAUI","cha":"BHZ","lat":-7.98, "lon":131.3, "label":"Tanibar",
     "thr_on":7.5,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0},
    {"net":"GE","sta":"BNDI","cha":"BHZ","lat":-4.52, "lon":129.9, "label":"BandaNeira",
     "thr_on":6.5,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0},

    # Papua
    {"net":"GE","sta":"FAKI","cha":"BHZ","lat":-2.92, "lon":132.25,"label":"Fak-Fak",
     "thr_on":6.5,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0},


    {"net":"AU","sta":"XMI", "cha":"BHZ","lat":-10.497,"lon":105.630,"label":"Christmas Island",
     "thr_on":5.0,"thr_off":0.6,"sta_sec":0.5,"lta_sec":12.0,"server":"iris"},
]


# ═══════════════════════════════════════════════════════════════════════════════
# PARAMETER GLOBAL EWS
# ═══════════════════════════════════════════════════════════════════════════════

GEOFON_HOST  = "geofon.gfz-potsdam.de"
GEOFON_PORT  = 18000
IRIS_HOST    = "rtserve.iris.washington.edu"
IRIS_PORT    = 18000

STA_SEC      = 0.5
LTA_SEC      = 12.0
MIN_STATIONS = 4         
ASSOC_WINDOW = 120       
EARTH_R      = 6371.0
GRID_POINTS  = 10000
GRID_RADIUS  = 25.0
DEPTH_CANDIDATES = [5, 10, 15, 20, 30, 50, 70, 100, 150]

EVENT_LIFETIME      = 180
ASSOC_TOLERANCE     = 60
INITIAL_TRIG_WINDOW = 120
MAX_FINGERPRINTS    = 100

#Java Single-Station Mode=

JAVA_STATIONS = {"BBJI", "UGM", "JAGI", "SMRI"}

JAVA_DEFAULT_LOCATIONS = {
    "BBJI": {"lat": -8.5, "lon": 107.5, "depth_km": 30},  # selatan Jawa Barat
    "UGM":  {"lat": -9.0, "lon": 110.0, "depth_km": 30},  # selatan Jawa Tengah
    "JAGI": {"lat": -9.5, "lon": 114.0, "depth_km": 35},  # selatan Jawa Timur
    "SMRI": {"lat": -9.0, "lon": 110.5, "depth_km": 30},  # selatan Jawa Tengah
}

SINGLE_STA_MIN_DURATION_SEC = 10.0


# ═══════════════════════════════════════════════════════════════════════════════
# TAUP CACHE
# ═══════════════════════════════════════════════════════════════════════════════

print("Pre-computing TauP cache...")
_TAUP_CACHE = {}

def get_taup_time(dist_deg, depth_km):
    dk  = round(dist_deg, 1)
    zk  = int(min(depth_km, 700))
    key = (dk, zk)
    if key in _TAUP_CACHE:
        return _TAUP_CACHE[key]
    try:
        arr = TAUP_MODEL.get_travel_times(zk, dk, ["P", "p"])
        t   = arr[0].time if arr else dk * 111.19 / 7.0
    except Exception:
        t = dk * 111.19 / 7.0
    _TAUP_CACHE[key] = t
    return t

for _d in np.arange(0.5, 25.0, 0.5):
    for _z in [5, 10, 20, 35, 70, 150]:
        get_taup_time(float(_d), float(_z))
print(f"TauP cache: {len(_TAUP_CACHE)} entries")


# ═══════════════════════════════════════════════════════════════════════════════
# KABUPATEN DATABASE (sama persis dengan V3)
# ═══════════════════════════════════════════════════════════════════════════════

KABUPATEN_DB = [
    ("Banda Aceh",5.548,95.323,"Aceh"),("Sabang",5.893,95.329,"Aceh"),
    ("Langsa",4.469,97.967,"Aceh"),("Lhokseumawe",5.180,97.150,"Aceh"),
    ("Meulaboh",4.136,96.129,"Aceh"),("Tapaktuan",3.267,97.175,"Aceh"),
    ("Medan",3.595,98.672,"Sumatera Utara"),("Binjai",3.600,98.486,"Sumatera Utara"),
    ("Pematangsiantar",2.959,99.068,"Sumatera Utara"),("Sibolga",1.742,98.779,"Sumatera Utara"),
    ("Padangsidimpuan",1.379,99.273,"Sumatera Utara"),("Gunungsitoli",1.288,97.607,"Sumatera Utara"),
    ("Padang",-0.950,100.354,"Sumatera Barat"),("Bukittinggi",-0.305,100.370,"Sumatera Barat"),
    ("Payakumbuh",-0.222,100.626,"Sumatera Barat"),("Solok",-0.795,100.654,"Sumatera Barat"),
    ("Painan",-1.357,100.578,"Sumatera Barat"),("Pekanbaru",0.533,101.450,"Riau"),
    ("Dumai",1.672,101.454,"Riau"),("Tembilahan",-0.350,103.167,"Riau"),
    ("Tanjungpinang",0.917,104.467,"Kepulauan Riau"),("Batam",1.100,104.017,"Kepulauan Riau"),
    ("Jambi",-1.600,103.617,"Jambi"),("Sungaipenuh",-2.083,101.383,"Jambi"),
    ("Palembang",-2.917,104.750,"Sumatera Selatan"),("Prabumulih",-3.426,104.239,"Sumatera Selatan"),
    ("Lubuklinggau",-3.294,102.857,"Sumatera Selatan"),("Lahat",-3.800,103.533,"Sumatera Selatan"),
    ("Bengkulu",-3.800,102.267,"Bengkulu"),("Curup",-3.467,102.517,"Bengkulu"),
    ("Bandar Lampung",-5.429,105.262,"Lampung"),("Metro",-5.113,105.306,"Lampung"),
    ("Kotabumi",-4.833,104.900,"Lampung"),("Liwa",-4.617,103.917,"Lampung"),
    ("Pangkalpinang",-2.133,106.117,"Bangka Belitung"),("Tanjungpandan",-2.750,107.633,"Bangka Belitung"),
    ("Jakarta Pusat",-6.186,106.826,"DKI Jakarta"),("Serang",-6.117,106.150,"Banten"),
    ("Tangerang",-6.179,106.630,"Banten"),("Pandeglang",-6.300,106.100,"Banten"),
    ("Bandung",-6.914,107.609,"Jawa Barat"),("Bekasi",-6.235,106.992,"Jawa Barat"),
    ("Bogor",-6.596,106.816,"Jawa Barat"),("Sukabumi",-6.921,106.927,"Jawa Barat"),
    ("Cirebon",-6.706,108.557,"Jawa Barat"),("Tasikmalaya",-7.327,108.220,"Jawa Barat"),
    ("Garut",-7.233,107.900,"Jawa Barat"),("Cianjur",-6.817,107.133,"Jawa Barat"),
    ("Subang",-6.567,107.750,"Jawa Barat"),("Karawang",-6.317,107.317,"Jawa Barat"),
    ("Indramayu",-6.327,108.322,"Jawa Barat"),("Kuningan",-6.975,108.483,"Jawa Barat"),
    ("Semarang",-6.967,110.417,"Jawa Tengah"),("Surakarta",-7.557,110.832,"Jawa Tengah"),
    ("Magelang",-7.467,110.217,"Jawa Tengah"),("Pekalongan",-6.889,109.676,"Jawa Tengah"),
    ("Tegal",-6.867,109.133,"Jawa Tengah"),("Cilacap",-7.733,109.017,"Jawa Tengah"),
    ("Banyumas",-7.533,109.300,"Jawa Tengah"),("Purbalingga",-7.383,109.367,"Jawa Tengah"),
    ("Kebumen",-7.667,109.650,"Jawa Tengah"),("Wonosobo",-7.367,109.900,"Jawa Tengah"),
    ("Banjarnegara",-7.367,109.700,"Jawa Tengah"),("Purworejo",-7.717,110.017,"Jawa Tengah"),
    ("Klaten",-7.700,110.600,"Jawa Tengah"),("Wonogiri",-7.817,110.917,"Jawa Tengah"),
    ("Blora",-6.967,111.417,"Jawa Tengah"),("Rembang",-6.700,111.333,"Jawa Tengah"),
    ("Pati",-6.750,111.033,"Jawa Tengah"),("Jepara",-6.583,110.667,"Jawa Tengah"),
    ("Yogyakarta",-7.797,110.370,"DI Yogyakarta"),("Wonosari",-7.967,110.600,"DI Yogyakarta"),
    ("Surabaya",-7.250,112.750,"Jawa Timur"),("Malang",-7.967,112.633,"Jawa Timur"),
    ("Kediri",-7.817,112.017,"Jawa Timur"),("Madiun",-7.633,111.517,"Jawa Timur"),
    ("Jember",-8.167,113.700,"Jawa Timur"),("Banyuwangi",-8.217,114.367,"Jawa Timur"),
    ("Lumajang",-8.133,113.217,"Jawa Timur"),("Pacitan",-8.200,111.100,"Jawa Timur"),
    ("Trenggalek",-8.050,111.700,"Jawa Timur"),("Tulungagung",-8.067,111.900,"Jawa Timur"),
    ("Blitar",-8.100,112.167,"Jawa Timur"),("Ponorogo",-7.867,111.500,"Jawa Timur"),
    ("Probolinggo",-7.750,113.217,"Jawa Timur"),("Situbondo",-7.700,114.017,"Jawa Timur"),
    ("Bondowoso",-7.917,113.833,"Jawa Timur"),("Bojonegoro",-7.150,111.883,"Jawa Timur"),
    ("Tuban",-6.900,112.050,"Jawa Timur"),("Lamongan",-7.117,112.417,"Jawa Timur"),
    ("Bangkalan",-7.050,112.733,"Jawa Timur"),("Sumenep",-6.983,113.867,"Jawa Timur"),
    ("Denpasar",-8.650,115.217,"Bali"),("Singaraja",-8.117,115.083,"Bali"),
    ("Tabanan",-8.533,115.117,"Bali"),("Negara",-8.367,114.633,"Bali"),
    ("Mataram",-8.583,116.117,"NTB"),("Bima",-8.467,118.717,"NTB"),
    ("Sumbawa Besar",-8.483,117.417,"NTB"),
    ("Kupang",-10.167,123.600,"NTT"),("Ende",-8.833,121.633,"NTT"),
    ("Maumere",-8.617,122.217,"NTT"),("Larantuka",-8.350,122.983,"NTT"),
    ("Waingapu",-9.667,120.250,"NTT"),("Bajawa",-8.783,120.967,"NTT"),
    ("Ruteng",-8.617,120.467,"NTT"),("Labuan Bajo",-8.500,119.883,"NTT"),
    ("Atambua",-9.100,124.900,"NTT"),("Soe",-9.850,124.283,"NTT"),
    ("Pontianak",-0.017,109.333,"Kalimantan Barat"),("Singkawang",0.900,108.983,"Kalimantan Barat"),
    ("Sambas",1.367,109.300,"Kalimantan Barat"),("Ketapang",-1.833,110.000,"Kalimantan Barat"),
    ("Palangkaraya",-2.207,113.921,"Kalimantan Tengah"),("Sampit",-2.533,112.950,"Kalimantan Tengah"),
    ("Pangkalan Bun",-2.683,111.633,"Kalimantan Tengah"),
    ("Banjarmasin",-3.317,114.583,"Kalimantan Selatan"),("Banjarbaru",-3.433,114.833,"Kalimantan Selatan"),
    ("Kotabaru",-3.300,116.167,"Kalimantan Selatan"),
    ("Samarinda",-0.500,117.150,"Kalimantan Timur"),("Balikpapan",-1.267,116.833,"Kalimantan Timur"),
    ("Bontang",0.133,117.500,"Kalimantan Timur"),("Tenggarong",-0.433,117.000,"Kalimantan Timur"),
    ("Tanjung Redeb",2.150,117.483,"Kalimantan Timur"),
    ("Tanjung Selor",2.833,117.367,"Kalimantan Utara"),("Tarakan",3.300,117.633,"Kalimantan Utara"),
    ("Manado",1.487,124.840,"Sulawesi Utara"),("Bitung",1.450,125.183,"Sulawesi Utara"),
    ("Kotamobagu",0.733,124.317,"Sulawesi Utara"),
    ("Gorontalo",0.550,123.067,"Gorontalo"),
    ("Palu",-0.900,119.867,"Sulawesi Tengah"),("Poso",-1.383,120.750,"Sulawesi Tengah"),
    ("Luwuk",-0.950,122.783,"Sulawesi Tengah"),("Toli-Toli",1.033,120.800,"Sulawesi Tengah"),
    ("Parigi",-0.783,120.183,"Sulawesi Tengah"),
    ("Mamuju",-2.683,118.883,"Sulawesi Barat"),("Majene",-3.533,118.967,"Sulawesi Barat"),
    ("Makassar",-5.133,119.417,"Sulawesi Selatan"),("Parepare",-4.017,119.633,"Sulawesi Selatan"),
    ("Palopo",-3.000,120.200,"Sulawesi Selatan"),("Bulukumba",-5.550,120.200,"Sulawesi Selatan"),
    ("Watampone",-4.533,120.333,"Sulawesi Selatan"),("Sengkang",-4.133,120.017,"Sulawesi Selatan"),
    ("Kendari",-3.967,122.517,"Sulawesi Tenggara"),("Bau-Bau",-5.467,122.617,"Sulawesi Tenggara"),
    ("Kolaka",-4.050,121.583,"Sulawesi Tenggara"),
    ("Ambon",-3.700,128.167,"Maluku"),("Tual",-5.633,132.750,"Maluku"),
    ("Saumlaki",-7.983,131.300,"Maluku"),("Masohi",-3.333,128.917,"Maluku"),
    ("Namlea",-3.250,127.100,"Maluku"),
    ("Ternate",0.783,127.383,"Maluku Utara"),("Tobelo",1.733,128.017,"Maluku Utara"),
    ("Sofifi",0.733,127.567,"Maluku Utara"),("Sanana",-2.067,125.983,"Maluku Utara"),
    ("Manokwari",-0.867,134.083,"Papua Barat"),("Sorong",-0.883,131.267,"Papua Barat"),
    ("Fakfak",-2.917,132.300,"Papua Barat"),("Kaimana",-3.650,133.750,"Papua Barat"),
    ("Jayapura",-2.533,140.717,"Papua"),("Merauke",-8.483,140.400,"Papua"),
    ("Nabire",-3.367,135.483,"Papua"),("Wamena",-4.083,138.950,"Papua"),
    ("Biak",-1.183,136.083,"Papua"),("Timika",-4.533,136.883,"Papua"),
]
_KAB_ARR = np.array([(k[1], k[2]) for k in KABUPATEN_DB])


# ═══════════════════════════════════════════════════════════════════════════════
# GEO HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def haversine_deg(la1, lo1, la2, lo2):
    r = math.pi / 180
    dla = (la2-la1)*r; dlo = (lo2-lo1)*r
    a = math.sin(dla/2)**2 + math.cos(la1*r)*math.cos(la2*r)*math.sin(dlo/2)**2
    return 2*math.degrees(math.asin(math.sqrt(max(0, min(1, a)))))

def dist_km(la1, lo1, la2, lo2):
    return haversine_deg(la1, lo1, la2, lo2) * 111.19

def bearing_str(la1, lo1, la2, lo2):
    r = math.pi/180; dlo = (lo2-lo1)*r
    y = math.sin(dlo)*math.cos(la2*r)
    x = math.cos(la1*r)*math.sin(la2*r) - math.sin(la1*r)*math.cos(la2*r)*math.cos(dlo)
    d = (math.degrees(math.atan2(y, x)) + 360) % 360
    return ["Utara","Timur Laut","Timur","Tenggara",
            "Selatan","Barat Daya","Barat","Barat Laut"][int((d+22.5)/45)%8]

def nearest_kabupaten(lat, lon):
    dlat = _KAB_ARR[:,0] - lat
    dlon = (_KAB_ARR[:,1] - lon) * math.cos(math.radians(lat))
    idx  = int(np.argmin(dlat**2 + dlon**2))
    k    = KABUPATEN_DB[idx]
    km   = dist_km(lat, lon, k[1], k[2])
    dr   = bearing_str(lat, lon, k[1], k[2])
    return k[0], k[3], round(km, 1), dr

PHI2 = 2.618033989

def move_on_globe(lat_r, lon_r, ang, dist_r):
    cl=math.cos(lat_r); sl=math.sin(lat_r)
    clo=math.cos(lon_r); slo=math.sin(lon_r)
    cd=math.cos(dist_r); sd=math.sin(dist_r)
    cg=math.cos(ang);    sg=math.sin(ang)
    x=cd*cl*clo-sd*(sl*clo*cg+slo*sg)
    y=cd*cl*slo-sd*(sl*slo*cg-clo*sg)
    z=sd*cl*cg+cd*sl
    z=max(-1.0, min(1.0, z))
    return math.asin(z), math.atan2(y, x)

def azimuth_gap(triggers, epicenter):
    azimuths = []
    for tr in triggers:
        dlon = math.radians(tr["lon"] - epicenter["lon"])
        y = math.sin(dlon)*math.cos(math.radians(tr["lat"]))
        x = (math.cos(math.radians(epicenter["lat"]))*math.sin(math.radians(tr["lat"])) -
             math.sin(math.radians(epicenter["lat"]))*math.cos(math.radians(tr["lat"]))*math.cos(dlon))
        az = (math.degrees(math.atan2(y, x)) + 360) % 360
        azimuths.append(az)
    azimuths.sort()
    gaps = [azimuths[i+1]-azimuths[i] for i in range(len(azimuths)-1)]
    gaps.append(360 - azimuths[-1] + azimuths[0])
    return max(gaps)

def is_likely_teleseismic(triggers, epicenter):
    if len(triggers) < 3:
        return False
    dists = [dist_km(epicenter["lat"], epicenter["lon"], t["lat"], t["lon"]) for t in triggers]
    min_dist = min(dists); max_dist = max(dists)
    if min_dist > 300:
        print(f"[FILTER] Stasiun terdekat {min_dist:.0f} km — kemungkinan teleseismik")
        return True
    t_arrive = sorted([t["t_arrive"] for t in triggers])
    time_spread = t_arrive[-1] - t_arrive[0]
    sta_dist_spread = max_dist - min_dist
    if time_spread < 15 and sta_dist_spread > 800:
        print(f"[FILTER] Time spread {time_spread:.1f}s, sta spread {sta_dist_spread:.0f}km — teleseismik")
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════════════════════

sta_cfg = {s["sta"]: s for s in STATIONS}

sta_buffers = {s["sta"]: {
    "data":          collections.deque(maxlen=30000),
    "sr":            20.0,
    "triggered":     False,
    "trigger_time":  None,
    "peak_amp":      0.0,
    "peak_cft":      0.0,   
    "trigger_start": None,   
    "reset_at":      None,
} for s in STATIONS}

lock          = threading.Lock()
active_events = {}
event_lock    = threading.Lock()
connected_ws  = set()

_processed_fingerprints = collections.deque(maxlen=MAX_FINGERPRINTS)


# ═══════════════════════════════════════════════════════════════════════════════
# GRID SEARCH & MAGNITUDE
# ═══════════════════════════════════════════════════════════════════════════════

def _residual(triggers, lat_r, lon_r, depth_km):
    la = math.degrees(lat_r); lo = math.degrees(lon_r)
    ots = []
    for tr in triggers:
        d  = haversine_deg(la, lo, tr["lat"], tr["lon"])
        tt = get_taup_time(d, depth_km)
        ots.append(tr["t_arrive"] - tt)
    ot = float(np.median(ots))
    sq = sum(
        (ot + get_taup_time(haversine_deg(la, lo, tr["lat"], tr["lon"]), depth_km) - tr["t_arrive"])**2
        for tr in triggers
    )
    return sq, ot


def spiral_search(triggers):
    if len(triggers) < 1:
        return None

    r    = math.pi / 180
    la0  = np.mean([t["lat"] for t in triggers]) * r
    lo0  = np.mean([t["lon"] for t in triggers]) * r
    max_r = math.radians(GRID_RADIUS)

    best_err = float("inf"); best = (None, None, None, None)

    for i in range(GRID_POINTS):
        ang = 2*math.pi*i/PHI2
        d   = math.sqrt(i)*(max_r/math.sqrt(GRID_POINTS-1))
        la, lo = move_on_globe(la0, lo0, ang, d)
        for z in DEPTH_CANDIDATES:
            err, ot = _residual(triggers, la, lo, z)
            if err < best_err:
                best_err = err; best = (la, lo, z, ot)

    if best[0] is None:
        return None

    fine_r = math.radians(3.0)
    for i in range(3000):
        ang = 2*math.pi*i/PHI2
        d   = math.sqrt(i)*(fine_r/math.sqrt(2999))
        la, lo = move_on_globe(best[0], best[1], ang, d)
        for z in [max(1, best[2]-15), max(1, best[2]-5), best[2], best[2]+5, best[2]+15]:
            err, ot = _residual(triggers, la, lo, z)
            if err < best_err:
                best_err = err; best = (la, lo, z, ot)

    elat = math.degrees(best[0]); elon = math.degrees(best[1])
    rms  = math.sqrt(best_err / len(triggers))

    return {"lat": round(elat, 3), "lon": round(elon, 3),
            "depth_km": round(float(best[2]), 1), "origin_t": float(best[3]),
            "rms_sec": round(rms, 2), "conf_km": round(rms*8, 1),
            "n_sta": len(triggers)}


def estimate_mag(triggers, epi):
    mls = []
    for tr in triggers:
        if tr["peak_amp"] <= 0:
            continue
        d  = dist_km(epi["lat"], epi["lon"], tr["lat"], tr["lon"])
        d  = max(d, 1.0); delta = d / 111.19
        ml = math.log10(tr["peak_amp"]) + 3*math.log10(8.0*delta) - 2.92
        mls.append(ml)
    return round(float(np.median(mls)), 1) if mls else None


def mmi_info(mag, depth):
    if mag is None:
        return "I", "Tidak terasa"
    v = max(1, min(10, round(1.5*mag - 0.5*math.log10(max(depth, 1)) - 1.0)))
    tbl = {1:("I","Tidak terasa"),2:("II","Sangat lemah"),3:("III","Lemah"),
           4:("IV","Cukup terasa"),5:("V","Kuat"),6:("VI","Sangat kuat"),
           7:("VII","Kerusakan ringan"),8:("VIII","Kerusakan sedang"),
           9:("IX","Kerusakan berat"),10:("X","Kerusakan sangat berat")}
    return tbl.get(v, ("I", "Tidak terasa"))


def alert_level(mag, n_sta, single_station=False):
    """
    Alert level dengan aturan single-station:
    Event dari 1 stasiun dikunci di level 1 (Deteksi Awal)
    sampai dikonfirmasi stasiun lain.
    """
    if mag is None or mag < 2.5 or n_sta < 1:
        return 0
    if single_station:
        # Single-station
        return 1
    if mag >= 6.5 and n_sta >= 4: return 4
    if mag >= 5.0 and n_sta >= 4: return 3
    if mag >= 4.0 and n_sta >= 3: return 2
    if mag >= 3.0 and n_sta >= 3: return 1
    return 0


def potential(mag, depth):
    if mag is None:
        return "Dalam analisis"
    if mag >= 7.0 and depth <= 70:  return "Berpotensi tsunami"
    if mag >= 6.5:                   return "Berpotensi merusak"
    if mag >= 5.0:                   return "Dapat dirasakan luas"
    if mag >= 4.0:                   return "Dapat dirasakan lokal"
    return "Umumnya tidak dirasakan"


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLE-STATION MODE UNTUK JAWA
# ═══════════════════════════════════════════════════════════════════════════════

def check_java_single_station():
    """
    Cek apakah ada stasiun Jawa yang memenuhi kriteria single-station trigger.

    Kriteria (SEMUA harus terpenuhi):
    1. Hanya stasiun Jawa (BBJI/UGM/JAGI/SMRI)
    2. peak_cft >= thr_single stasiun tersebut
    3. Durasi trigger >= SINGLE_STA_MIN_DURATION_SEC (anti-noise-burst)
    4. TIDAK ada stasiun Jawa lain yang juga trigger
       (jika ada 2+ stasiun, gunakan mode normal saja)
    5. Tidak ada event aktif yang sudah mencakup stasiun ini

    Return: dict trigger atau None
    """
    now = time.time()
    java_triggered = []

    with lock:
        for sta in JAVA_STATIONS:
            buf = sta_buffers.get(sta)
            if buf is None:
                continue
            if not buf["triggered"] or buf["trigger_time"] is None:
                continue
            # Cek apakah sudah dalam ASSOC_WINDOW
            if now - buf["trigger_time"] > ASSOC_WINDOW:
                continue

            cfg        = sta_cfg.get(sta, {})
            thr_single = cfg.get("thr_single", 999.0)
            peak_cft   = buf["peak_cft"]

            if peak_cft < thr_single:
                continue  

            # Cek durasi
            t_start = buf.get("trigger_start", buf["trigger_time"])
            duration = now - t_start
            if duration < SINGLE_STA_MIN_DURATION_SEC:
                continue 

            java_triggered.append({
                "sta":       sta,
                "lat":       cfg["lat"],
                "lon":       cfg["lon"],
                "label":     cfg["label"],
                "t_arrive":  buf["trigger_time"],
                "peak_amp":  buf["peak_amp"],
                "peak_cft":  peak_cft,
                "duration":  duration,
            })

   
    if len(java_triggered) != 1:
        return None

    tr = java_triggered[0]

    with event_lock:
        for ev in active_events.values():
            if isinstance(ev, dict) and tr["sta"] in ev.get("stations", []):
                return None  

    print(f"[JAVA-1STA] {tr['sta']} peak_cft={tr['peak_cft']:.1f} duration={tr['duration']:.0f}s → trigger single-station")
    return tr


def finalize_single_station_event(trigger):
    """
     event dari 1 stasiun Jawa.
  
    """
    sta       = trigger["sta"]
    default   = JAVA_DEFAULT_LOCATIONS[sta]
    epi_lat   = default["lat"]
    epi_lon   = default["lon"]
    depth_km  = default["depth_km"]


    d_deg  = haversine_deg(epi_lat, epi_lon, trigger["lat"], trigger["lon"])
    tt     = get_taup_time(d_deg, depth_km)
    origin = trigger["t_arrive"] - tt

    # Magnitude dari amplitudo
    d   = dist_km(epi_lat, epi_lon, trigger["lat"], trigger["lon"])
    d   = max(d, 1.0)
    mag = None
    if trigger["peak_amp"] > 0:
        delta = d / 111.19
        ml    = math.log10(trigger["peak_amp"]) + 3*math.log10(8.0*delta) - 2.92
        mag   = round(float(ml), 1)

  
    if mag is not None and mag < 4.0:
        print(f"[JAVA-1STA] Magnitude estimasi M{mag} < M4.0, abaikan")
        return None

    mmi, mmi_d = mmi_info(mag, depth_km)
    lvl        = alert_level(mag, 1, single_station=True) 
    kab, prov, kd, kdir = nearest_kabupaten(epi_lat, epi_lon)
    pot        = potential(mag, depth_km)

    lat_s = f"{abs(epi_lat):.2f}°{'LS' if epi_lat<0 else 'LU'}"
    lon_s = f"{abs(epi_lon):.2f}°{'BT' if epi_lon>0 else 'BB'}"
    wilayah = f"{kd:.0f} km {kdir} {kab}, {prov}"

    ev_id = f"java1_{sta}_{int(origin/30)}"

    with event_lock:
        if ev_id in active_events:
            return None

    ev = {
        "id":            ev_id,
        "lat":           epi_lat,
        "lon":           epi_lon,
        "lat_str":       lat_s,
        "lon_str":       lon_s,
        "depth_km":      depth_km,
        "magnitude":     mag,
        "mmi":           mmi,
        "mmi_desc":      mmi_d,
        "wilayah":       wilayah,
        "kabupaten":     kab,
        "provinsi":      prov,
        "kab_dist_km":   kd,
        "kab_dir":       kdir,
        "potential":     pot,
        "alert_level":   lvl,
        "alert_label":   "Deteksi Awal (Konfirmasi Pending)",
        "n_stations":    1,
        "stations":      [sta],
        "rms_sec":       None,
        "conf_km":       None,
        "origin_time":   origin,
        "timestamp":     time.time(),
        "last_update":   time.time(),
        # Flag single-station
        "single_station":     True,
        "single_station_sta": sta,
     
        "locked_lat":    epi_lat,
        "locked_lon":    epi_lon,
        "locked_depth":  depth_km,
        "locked_origin": origin,
 
        "note": (f"Terdeteksi oleh 1 stasiun ({trigger['label']}). "
                 f"Lokasi perkiraan zona subduksi. "
                 f"Menunggu konfirmasi stasiun lain."),
    }

    print(f"[JAVA-1STA] Event baru: M{mag} perkiraan {wilayah} | sta={sta} cft={trigger['peak_cft']:.1f}")
    return ev


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT MANAGEMENT (sama dengan V3 + modifikasi kecil)
# ═══════════════════════════════════════════════════════════════════════════════

def build_event_triggers(station_list):
    trigs = []
    now   = time.time()
    for sta in station_list:
        if sta not in sta_buffers or sta not in sta_cfg:
            continue
        buf = sta_buffers[sta]
        if buf.get("triggered") and buf.get("trigger_time"):
            if now - buf["trigger_time"] <= ASSOC_WINDOW:
                trigs.append({
                    "sta":      sta,
                    "lat":      sta_cfg[sta]["lat"],
                    "lon":      sta_cfg[sta]["lon"],
                    "label":    sta_cfg[sta]["label"],
                    "t_arrive": buf["trigger_time"],
                    "peak_amp": buf["peak_amp"],
                })
    return trigs


def find_event_by_arrival(trigger):
    now = time.time()
    with event_lock:
        for ev_id, ev in active_events.items():
            if not isinstance(ev, dict):
                continue
            if now - ev.get("origin_time", 0) > EVENT_LIFETIME:
                continue
            if trigger["sta"] in ev.get("stations", []):
                return ev_id
            d  = dist_km(ev["lat"], ev["lon"], trigger["lat"], trigger["lon"])
            tt = get_taup_time(d/111.19, ev["depth_km"])
            if abs(trigger["t_arrive"] - (ev["origin_time"] + tt)) < ASSOC_TOLERANCE:
                return ev_id
    return None


def merge_to_event(ev_id, new_triggers):
    """
    Masukkan trigger baru ke event existing.
    Jika event tadinya single-station dan sekarang dapat konfirmasi:
    - Hapus flag single_station
    - Upgrade alert_level sesuai magnitude baru
    """
    with event_lock:
        ev = active_events.get(ev_id)
        if not ev or not isinstance(ev, dict):
            return None

        added = False
        for tr in new_triggers:
            if tr["sta"] not in ev["stations"]:
                ev["stations"].append(tr["sta"])
                added = True

        if not added:
            return ev

        all_trigs = build_event_triggers(ev["stations"])
        loc = {
            "lat":      ev.get("locked_lat", ev["lat"]),
            "lon":      ev.get("locked_lon", ev["lon"]),
            "depth_km": ev.get("locked_depth", ev["depth_km"]),
        }

        new_mag = estimate_mag(all_trigs, loc)
        n_sta   = len(ev["stations"])

        if new_mag:
            ev["magnitude"]  = round(new_mag, 1)
            ev["mmi"], ev["mmi_desc"] = mmi_info(ev["magnitude"], loc["depth_km"])
            ev["potential"]  = potential(ev["magnitude"], loc["depth_km"])
            ev["n_stations"] = n_sta

            # Upgrade dari single-station jika sekarang ada 2+ stasiun
            was_single = ev.get("single_station", False)
            if was_single and n_sta >= 2:
                ev["single_station"] = False
                ev["alert_label"]    = ["Deteksi Awal","Konfirmasi","Gempa Sedang",
                                         "Gempa Kuat","Gempa Sangat Kuat"][
                                         alert_level(ev["magnitude"], n_sta)]
                ev["alert_level"]    = alert_level(ev["magnitude"], n_sta)
                ev["note"]           = f"Dikonfirmasi oleh {n_sta} stasiun."
                print(f"[EWS] Single-station {ev_id} UPGRADE → M{ev['magnitude']} {n_sta} stasiun")
            else:
                ev["alert_level"] = alert_level(
                    ev["magnitude"], n_sta, single_station=ev.get("single_station", False))
                ev["alert_label"] = (
                    "Deteksi Awal (Konfirmasi Pending)" if ev.get("single_station") else
                    ["Deteksi Awal","Konfirmasi","Gempa Sedang",
                     "Gempa Kuat","Gempa Sangat Kuat"][ev["alert_level"]]
                )

        ev["last_update"] = time.time()
        return ev


def finalize_new_event(triggers):
    """Buat event baru dari trigger multi-stasiun (normal mode)."""
    if len(triggers) < MIN_STATIONS:
        return None

    print(f"[EWS] Finalize event dari {len(triggers)} stasiun...")
    epi = spiral_search(triggers)
    if epi is None:
        print("[EWS] Spiral search gagal")
        return None

    if not (-15 <= epi["lat"] <= 10 and 90 <= epi["lon"] <= 145):
        print(f"[EWS] Lokasi di luar area ({epi['lat']:.1f},{epi['lon']:.1f})")
        return None

    if epi["rms_sec"] > 25:
        print(f"[EWS] RMS terlalu besar {epi['rms_sec']:.1f}s")
        return None

    if is_likely_teleseismic(triggers, {"lat": epi["lat"], "lon": epi["lon"]}):
        print("[EWS] Ditolak: kemungkinan teleseismik")
        return None

    min_dist_to_sta = min(dist_km(epi["lat"], epi["lon"], t["lat"], t["lon"]) for t in triggers)
    if min_dist_to_sta < 150 and epi["depth_km"] > 50:
        print(f"[EWS] Force shallow depth: {epi['depth_km']} → 15 km")
        epi["depth_km"] = 15.0

    mag        = estimate_mag(triggers, epi)
    mmi, mmi_d = mmi_info(mag, epi["depth_km"])
    lvl        = alert_level(mag if mag else 0, epi["n_sta"])
    kab, prov, kd, kdir = nearest_kabupaten(epi["lat"], epi["lon"])
    pot        = potential(mag, epi["depth_km"])

    lat_s   = f"{abs(epi['lat']):.2f}°{'LS' if epi['lat']<0 else 'LU'}"
    lon_s   = f"{abs(epi['lon']):.2f}°{'BT' if epi['lon']>0 else 'BB'}"
    wilayah = f"{kd:.0f} km {kdir} {kab}, {prov}"

    ev_id = str(int(round(epi["origin_t"] / 30)))

    with event_lock:
        if isinstance(active_events.get(ev_id), dict):
            print(f"[EWS] Event {ev_id} sudah ada, akan di-merge")
            return None

    ev = {
        "id":           ev_id,
        "lat":          epi["lat"],
        "lon":          epi["lon"],
        "lat_str":      lat_s,
        "lon_str":      lon_s,
        "depth_km":     epi["depth_km"],
        "magnitude":    mag,
        "mmi":          mmi,
        "mmi_desc":     mmi_d,
        "wilayah":      wilayah,
        "kabupaten":    kab,
        "provinsi":     prov,
        "kab_dist_km":  kd,
        "kab_dir":      kdir,
        "potential":    pot,
        "alert_level":  lvl,
        "alert_label":  ["Deteksi Awal","Konfirmasi","Gempa Sedang",
                          "Gempa Kuat","Gempa Sangat Kuat"][lvl],
        "n_stations":   epi["n_sta"],
        "stations":     [t["sta"] for t in triggers],
        "rms_sec":      epi["rms_sec"],
        "conf_km":      epi["conf_km"],
        "origin_time":  epi["origin_t"],
        "timestamp":    time.time(),
        "last_update":  time.time(),
        "single_station": False,
        "locked_lat":   epi["lat"],
        "locked_lon":   epi["lon"],
        "locked_depth": epi["depth_km"],
        "locked_origin":epi["origin_t"],
    }

    print(f"[EWS] LOCKED M{mag} {wilayah} | depth={epi['depth_km']}km MMI={mmi} lvl={lvl}")
    return ev


# ═══════════════════════════════════════════════════════════════════════════════
# SEEDLINK CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class EWSClient(EasySeedLinkClient):

    def on_data(self, trace):
        sta = trace.stats.station
        if sta not in sta_buffers:
            return

        cfg = sta_cfg.get(sta, {})

        with lock:
            buf = sta_buffers[sta]
            sr  = float(trace.stats.sampling_rate)
            buf["sr"] = sr

            new_maxlen = int(sr * 300)
            if buf["data"].maxlen != new_maxlen:
                buf["data"] = collections.deque(buf["data"], maxlen=new_maxlen)
            for v in trace.data:
                buf["data"].append(float(v))

            arr = np.array(buf["data"])
            sri = int(sr)
            if len(arr) < sri * 15:
                return

            thr_on  = cfg.get("thr_on", 4.0)
            thr_off = cfg.get("thr_off", 0.7)
            nsta    = max(1, int(cfg.get("sta_sec", STA_SEC) * sri))
            nlta    = max(1, int(cfg.get("lta_sec", LTA_SEC) * sri))

            # ── Hitung STA/LTA ──
            try:
                arr_f = arr.astype(np.float64)
                if _QM_AVAILABLE:
                    from quakemigrate.core.lib import recursive_sta_lta
                    cft = recursive_sta_lta(arr_f, nsta, nlta)
                else:
                    cft = classic_sta_lta(arr_f, nsta, nlta)
            except Exception:
                cft = classic_sta_lta(arr.astype(np.float64), nsta, nlta)

            on_off = trigger_onset(cft, thr_on, thr_off)
            now    = time.time()

          
            recent_cft = cft[-sri*10:] if len(cft) >= sri*10 else cft
            buf["peak_cft"] = float(np.max(recent_cft)) if len(recent_cft) > 0 else 0.0

            if len(on_off) > 0 and not buf["triggered"]:
                buf["triggered"]     = True
                buf["trigger_time"]  = now
                buf["trigger_start"] = now   
                buf["peak_amp"]      = float(np.abs(arr[-sri*10:]).max())
                buf["reset_at"]      = now + 150
                print(f"[+] {sta} triggered @ {time.strftime('%H:%M:%S')} "
                      f"cft={buf['peak_cft']:.1f} amp={buf['peak_amp']:.0f}")

            # Auto reset 
            if buf["triggered"] and buf["reset_at"] and now > buf["reset_at"]:
                buf["triggered"]     = False
                buf["trigger_time"]  = None
                buf["trigger_start"] = None
                buf["peak_amp"]      = 0.0
                buf["reset_at"]      = None

    def on_seedlink_error(self):
        print("SeedLink error...")


# ═══════════════════════════════════════════════════════════════════════════════
# SEEDLINK THREADS
# ═══════════════════════════════════════════════════════════════════════════════

STATIONS_GEOFON = [s for s in STATIONS if s.get("server", "geofon") == "geofon"]
STATIONS_IRIS   = [s for s in STATIONS if s.get("server") == "iris"]


def _run_seedlink(host, port, station_list, server_name):
    while True:
        try:
            c = EWSClient(f"{host}:{port}")
            for s in station_list:
                try:
                    c.select_stream(s["net"], s["sta"], s["cha"])
                    print(f"  [{server_name}] sub: {s['sta']}")
                except Exception as e:
                    print(f"  [{server_name}] skip {s['sta']}: {e}")
            print(f"[{server_name}] Connected → {host}:{port}")
            c.run()
        except Exception as e:
            print(f"[{server_name}] Reconnect: {e}")
            time.sleep(10)


threading.Thread(
    target=lambda: _run_seedlink(GEOFON_HOST, GEOFON_PORT, STATIONS_GEOFON, "GEOFON"),
    daemon=True
).start()

threading.Thread(
    target=lambda: _run_seedlink(IRIS_HOST, IRIS_PORT, STATIONS_IRIS, "IRIS"),
    daemon=True
).start()


# ═══════════════════════════════════════════════════════════════════════════════
# TRIGGER PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════════

def collect_triggers():
    now = time.time()
    with lock:
        trigs = []
        for s in STATIONS:
            buf = sta_buffers[s["sta"]]
            if buf["triggered"] and buf["trigger_time"] and (now - buf["trigger_time"]) <= ASSOC_WINDOW:
                trigs.append({
                    "sta":      s["sta"],
                    "lat":      s["lat"],
                    "lon":      s["lon"],
                    "label":    s["label"],
                    "t_arrive": buf["trigger_time"],
                    "peak_amp": buf["peak_amp"],
                })
    return trigs


_processing = False


async def trigger_processor():
    global _processing

    while True:
        await asyncio.sleep(3)

        # ── 1. trigger aktif ──
        trigs = collect_triggers()

        # ── 2. Java single-station check
        if not _processing:
            java_single = check_java_single_station()
            if java_single is not None:
                fp = f"java1_{java_single['sta']}_{int(java_single['t_arrive']/30)}"
                if fp not in _processed_fingerprints:
                    _processed_fingerprints.append(fp)
                    _processing = True

                    def run_java_single(tr):
                        global _processing
                        try:
                            ev = finalize_single_station_event(tr)
                            if ev is None:
                                return
                            with event_lock:
                                active_events[ev["id"]] = ev
                            if connected_ws:
                                loop = asyncio.get_event_loop()
                                msg  = json.dumps({
                                    "type":    "ews_alert",
                                    "data":    ev,
                                    "update":  False,
                                    "message": (f"⚠ Deteksi Awal: M{ev['magnitude']} "
                                                f"{ev['wilayah']} — "
                                                f"terdeteksi 1 stasiun ({tr['label']}), "
                                                f"menunggu konfirmasi"),
                                })
                                for ws in connected_ws.copy():
                                    try:
                                        asyncio.run_coroutine_threadsafe(ws.send(msg), loop)
                                    except Exception:
                                        pass
                        finally:
                            _processing = False

                    threading.Thread(target=run_java_single, args=(java_single,), daemon=True).start()

        if len(trigs) < 2:
            continue

        # ── 3. Asosiasikan trigger ke event existing ──
        assoc_map    = {}
        unassociated = []

        for tr in trigs:
            ev_id = find_event_by_arrival(tr)
            if ev_id:
                assoc_map.setdefault(ev_id, []).append(tr)
            else:
                unassociated.append(tr)

        # ── 4. Update event existing ──
        for ev_id, trig_list in assoc_map.items():
            ev = merge_to_event(ev_id, trig_list)
            if ev and connected_ws:
                msg = json.dumps({
                    "type":    "ews_alert",
                    "data":    ev,
                    "update":  True,
                    "message": (f"Update: M{ev['magnitude']} dari "
                                f"{ev['n_stations']} stasiun"),
                })
                dead = set()
                for ws in connected_ws.copy():
                    try:
                        await ws.send(msg)
                    except Exception:
                        dead.add(ws)
                connected_ws.difference_update(dead)
                print(f"[EWS] UPDATE {ev_id}: M{ev['magnitude']} sta={ev['n_stations']}")

        # ── 5. event baru dari trigger yang belum terasosiasikan ──
        if len(unassociated) >= MIN_STATIONS and not _processing:
            unassociated.sort(key=lambda x: x["t_arrive"])
            first_arrival  = unassociated[0]["t_arrive"]
            initial_trigs  = [tr for tr in unassociated
                              if tr["t_arrive"] <= first_arrival + INITIAL_TRIG_WINDOW]

            if len(initial_trigs) < MIN_STATIONS:
                pass
            else:
                fp = (f"{int(first_arrival/30)}_"
                      f"{'_'.join(sorted(t['sta'] for t in initial_trigs))}")
                if fp not in _processed_fingerprints:
                    _processed_fingerprints.append(fp)
                    _processing = True

                    def run_finalize():
                        global _processing
                        try:
                            ev = finalize_new_event(initial_trigs)

                            if ev is None:
                               
                                approx_origin = initial_trigs[0]["t_arrive"] - 25
                                target_ev     = None
                                with event_lock:
                                    for eid, eobj in active_events.items():
                                        if (isinstance(eobj, dict) and
                                                abs(eobj.get("origin_time", 0) - approx_origin) < 45):
                                            target_ev = eid
                                            break
                                if target_ev:
                                    ev = merge_to_event(target_ev, initial_trigs)
                                    if ev and connected_ws:
                                        loop = asyncio.get_event_loop()
                                        msg  = json.dumps({
                                            "type":    "ews_alert",
                                            "data":    ev,
                                            "update":  True,
                                            "message": (f"Update: M{ev['magnitude']} "
                                                        f"dari {ev['n_stations']} stasiun"),
                                        })
                                        for ws in connected_ws.copy():
                                            try:
                                                asyncio.run_coroutine_threadsafe(
                                                    ws.send(msg), loop)
                                            except Exception:
                                                pass
                                return

                            with event_lock:
                                active_events[ev["id"]] = ev

                            if connected_ws:
                                loop = asyncio.get_event_loop()
                                msg  = json.dumps({
                                    "type":    "ews_alert",
                                    "data":    ev,
                                    "update":  False,
                                    "message": (f"Gempa Baru: M{ev['magnitude']} "
                                                f"{ev['wilayah']}"),
                                })
                                for ws in connected_ws.copy():
                                    try:
                                        asyncio.run_coroutine_threadsafe(
                                            ws.send(msg), loop)
                                    except Exception:
                                        pass
                        finally:
                            _processing = False

                    threading.Thread(target=run_finalize, daemon=True).start()

        # ── 6. Bersihkan event expired ──
        now = time.time()
        with event_lock:
            expired = [
                k for k, ev in active_events.items()
                if isinstance(ev, dict) and now - ev.get("origin_time", 0) > EVENT_LIFETIME
            ]
            for k in expired:
                print(f"[EWS] Expire event {k}")
                del active_events[k]


# ═══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

async def handler(websocket):
    connected_ws.add(websocket)
    print(f"[WS] Client: {websocket.remote_address}")

    try:
       
        with lock:
            sta_list = [{
                "sta":      s["sta"],
                "label":    s["label"],
                "lat":      s["lat"],
                "lon":      s["lon"],
                "triggered": sta_buffers[s["sta"]]["triggered"],
                "java":     s.get("java", False),    
                "thr_single": s.get("thr_single", None), 
            } for s in STATIONS]
        await websocket.send(json.dumps({"type": "station_status", "data": sta_list}))

 
        while True:
            with lock:
                sta_status = [{
                    "sta":      s["sta"],
                    "triggered": sta_buffers[s["sta"]]["triggered"],
                    "amp":      round(sta_buffers[s["sta"]]["peak_amp"], 1),
                    "cft":      round(sta_buffers[s["sta"]]["peak_cft"], 1), 
                } for s in STATIONS]

            trig_n = sum(1 for s in sta_status if s["triggered"])

            with event_lock:
                events_snapshot = [ev for ev in active_events.values()
                                   if isinstance(ev, dict)]

            await websocket.send(json.dumps({
                "type":      "heartbeat",
                "stations":  sta_status,
                "triggered": trig_n,
                "time":      time.time(),
                "active_events": len(events_snapshot),
            }))
            await asyncio.sleep(5)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_ws.discard(websocket)
        print("[WS] Client disconnected")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    print("[EWS] Waiting for SeedLink (20s)...")
    await asyncio.sleep(20)
    port = int(os.environ.get("PORT", 8766))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"[EWS] WebSocket EWS running on port {port}")
        asyncio.create_task(trigger_processor())
        await asyncio.Future()


threading.Thread(
    target=lambda: _run_seedlink(GEOFON_HOST, GEOFON_PORT, STATIONS_GEOFON, "GEOFON"),
    daemon=True
).start()

asyncio.run(main())
