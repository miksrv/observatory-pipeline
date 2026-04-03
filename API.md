# Observatory Pipeline — API Reference

This document describes all REST API endpoints consumed by the `observatory-pipeline` service.
The API is implemented in the companion **observatory-api** repository (CodeIgniter 4 / PHP).

The pipeline communicates with the API exclusively over HTTPS. It has **no direct database access** — all persistence goes through these endpoints.

---

## Base URL

```
https://<your-cloud-host>/api/v1
```

Configured via the `API_BASE_URL` environment variable.

---

## Authentication

Every request must include the following headers:

| Header | Value | Description |
|--------|-------|-------------|
| `X-API-Key` | `<your-api-key>` | Pipeline authentication key |
| `Content-Type` | `application/json` | Required for all POST requests |
| `Accept` | `application/json` | Expected response format |

The API key is configured via the `API_KEY` environment variable in `.env`.  
The observatory server's outbound IP should be whitelisted on the cloud firewall.

---

## Error Responses

All endpoints return standard HTTP status codes:

| Code | Meaning |
|------|---------|
| `200 OK` | Success (GET requests) |
| `201 Created` | Resource created successfully (POST requests) |
| `400 Bad Request` | Invalid request body or parameters |
| `401 Unauthorized` | Missing or invalid API key |
| `404 Not Found` | Resource not found |
| `422 Unprocessable Entity` | Validation error (details in response body) |
| `500 Internal Server Error` | Server-side error — pipeline will retry |

Error response body:

```json
{
  "error": "Human-readable error description",
  "details": {}
}
```

The pipeline retries on HTTP 5xx and transport errors (3 attempts, exponential backoff: 2s, 4s, 8s). HTTP 4xx errors are logged and not retried.

---

## Endpoints

### 1. Register a Frame

**[POST /frames](#1-register-a-frame)**

---

### 2. Save Sources for a Frame

**[POST /frames/{id}/sources](#2-save-sources-for-a-frame-1)**

---

### 3. Save Anomalies for a Frame

**[POST /frames/{id}/anomalies](#3-save-anomalies-for-a-frame-1)**

---

### 4. Get Historical Sources Near a Sky Position

**[GET /sources/near](#4-get-historical-sources-near-a-sky-position-1)**

---

### 5. Get Frames Covering a Sky Position

**[GET /frames/covering](#5-get-frames-covering-a-sky-position-1)**

---

---

## 1. Register a Frame

Registers a newly processed FITS frame with the API. Returns a `frame_id` that is used as a key for all subsequent calls for this frame (sources, anomalies).

### Request

```
POST /frames
```

**Headers:**

```
X-API-Key: <api-key>
Content-Type: application/json
Accept: application/json
```

**Body:**

```json
{
  "filename": "frame_20240315_220134.fits",
  "original_filepath": "/fits/archive/M51/frame_20240315_220134.fits",
  "obs_time": "2024-03-15T22:01:34Z",
  "ra_center": 202.4696,
  "dec_center": 47.1952,
  "fov_deg": 1.25,
  "quality_flag": "OK",

  "observation": {
    "object": "M51",
    "exptime": 120.0,
    "filter": "V",
    "frame_type": "Light",
    "airmass": 1.23
  },

  "instrument": {
    "telescope": "Celestron EdgeHD 11",
    "camera": "ZWO ASI2600MM Pro",
    "focal_length_mm": 2800,
    "aperture_mm": 280
  },

  "sensor": {
    "temp_celsius": -10.0,
    "temp_setpoint_celsius": -10.0,
    "binning_x": 1,
    "binning_y": 1,
    "gain": 100,
    "offset": 50,
    "width_px": 6248,
    "height_px": 4176
  },

  "observer": {
    "name": "John Smith",
    "site_name": "Backyard Observatory",
    "site_lat": 55.7558,
    "site_lon": 37.6173,
    "site_elev_m": 150
  },

  "software": {
    "capture": "N.I.N.A. 2.1"
  },

  "qc": {
    "fwhm_median": 3.2,
    "elongation": 1.1,
    "snr_median": 42.5,
    "sky_background": 850.3,
    "star_count": 287,
    "eccentricity": 0.4
  }
}
```

**Field descriptions:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `filename` | string | yes | Original FITS filename (basename only) |
| `original_filepath` | string | yes | Full path on the observatory server after archiving |
| `obs_time` | string (ISO 8601) | yes | Observation start time in UTC, e.g. `2024-03-15T22:01:34Z` |
| `ra_center` | float | yes | Right ascension of frame center in decimal degrees |
| `dec_center` | float | yes | Declination of frame center in decimal degrees |
| `fov_deg` | float | yes | Field of view (longest axis) in degrees, from plate solve |
| `quality_flag` | string | yes | Always `"OK"` — bad frames are never sent to the API |
| `observation.object` | string | no | Target name from `OBJECT` FITS header |
| `observation.exptime` | float | no | Exposure time in seconds |
| `observation.filter` | string | no | Filter name, e.g. `"V"`, `"R"`, `"Ha"`, `"Luminance"` |
| `observation.frame_type` | string | no | Frame type from `IMAGETYP` header: `Light`, `Dark`, `Flat`, `Bias` |
| `observation.airmass` | float | no | Atmospheric airmass at observation time |
| `instrument.telescope` | string | no | Telescope name from `TELESCOP` header |
| `instrument.camera` | string | no | Camera name from `INSTRUME` header |
| `instrument.focal_length_mm` | integer | no | Focal length in millimetres |
| `instrument.aperture_mm` | integer | no | Aperture diameter in millimetres |
| `sensor.temp_celsius` | float | no | Actual sensor temperature (°C) |
| `sensor.temp_setpoint_celsius` | float | no | Target sensor temperature (°C) |
| `sensor.binning_x` | integer | no | Horizontal pixel binning |
| `sensor.binning_y` | integer | no | Vertical pixel binning |
| `sensor.gain` | integer | no | Camera gain setting (e⁻/ADU) |
| `sensor.offset` | integer | no | Camera offset/bias level |
| `sensor.width_px` | integer | no | Image width in pixels |
| `sensor.height_px` | integer | no | Image height in pixels |
| `observer.name` | string | no | Observer name from `OBSERVER` header |
| `observer.site_name` | string | no | Observatory site name |
| `observer.site_lat` | float | no | Site latitude in decimal degrees (positive = North) |
| `observer.site_lon` | float | no | Site longitude in decimal degrees (positive = East) |
| `observer.site_elev_m` | integer | no | Site elevation in metres above sea level |
| `software.capture` | string | no | Capture software name from `SWCREATE` header |
| `qc.fwhm_median` | float | no | Median FWHM of detected stars in arcseconds |
| `qc.elongation` | float | no | Median star elongation (major/minor axis ratio) |
| `qc.snr_median` | float | no | Median signal-to-noise ratio of detected sources |
| `qc.sky_background` | float | no | Median sky background level (ADU) |
| `qc.star_count` | integer | no | Number of stars detected by QC step |
| `qc.eccentricity` | float | no | Median PSF eccentricity (0 = circular, 1 = linear) |

### Response

**Status: `201 Created`**

```json
{
  "id": "42",
  "message": "Frame registered successfully"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Frame ID assigned by the API — used in all subsequent calls for this frame |
| `message` | string | Human-readable confirmation |

**Error responses:**

| Status | When |
|--------|------|
| `400` | Missing required fields (`filename`, `obs_time`, `ra_center`, `dec_center`) |
| `401` | Invalid or missing `X-API-Key` |
| `422` | Field type validation failure (e.g. non-numeric coordinates) |

---

## 2. Save Sources for a Frame

Saves the list of detected and catalog-matched sources for a previously registered frame.

### Request

```
POST /frames/{id}/sources
```

**URL parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string | Frame ID returned by `POST /frames` |

**Headers:**

```
X-API-Key: <api-key>
Content-Type: application/json
Accept: application/json
```

**Body:**

```json
{
  "filename": "frame_20240315_220134.fits",
  "sources": [
    {
      "ra": 202.461,
      "dec": 47.182,
      "mag": 14.23,
      "flux": 45230.5,
      "fwhm": 3.1,
      "catalog_name": "Gaia DR3",
      "catalog_id": "Gaia DR3 1234567890123456789",
      "catalog_mag": 14.15,
      "object_type": "STAR"
    },
    {
      "ra": 202.478,
      "dec": 47.201,
      "mag": 16.85,
      "flux": 8742.1,
      "fwhm": 3.3,
      "catalog_name": "Simbad",
      "catalog_id": "V* RR Lyr",
      "catalog_mag": null,
      "object_type": "V*"
    },
    {
      "ra": 202.490,
      "dec": 47.195,
      "mag": 18.42,
      "flux": 1205.7,
      "fwhm": 3.0,
      "catalog_name": null,
      "catalog_id": null,
      "catalog_mag": null,
      "object_type": null
    }
  ]
}
```

**Field descriptions:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `filename` | string | yes | FITS filename — included for logging and correlation |
| `sources` | array | yes | List of detected source objects. An empty array `[]` is valid |
| `sources[].ra` | float | yes | Source right ascension in decimal degrees |
| `sources[].dec` | float | yes | Source declination in decimal degrees |
| `sources[].mag` | float | no | Calibrated (or instrumental) magnitude |
| `sources[].flux` | float | no | Raw aperture flux in ADU |
| `sources[].fwhm` | float | no | FWHM of the source PSF in arcseconds |
| `sources[].catalog_name` | string\|null | no | Matched catalog: `"Gaia DR3"`, `"Simbad"`, `"MPC"`, or `null` if unmatched |
| `sources[].catalog_id` | string\|null | no | Catalog object identifier (Gaia source_id, Simbad MAIN_ID, MPC designation) |
| `sources[].catalog_mag` | float\|null | no | Reference magnitude from catalog (Gaia G-band), or `null` |
| `sources[].object_type` | string\|null | no | Object type: `"STAR"`, Simbad OTYPE string, `"ASTEROID"`, `"COMET"`, or `null` |

### Response

**Status: `200 OK`** or **`201 Created`**

```json
{
  "message": "Sources saved successfully",
  "count": 3
}
```

| Field | Type | Description |
|-------|------|-------------|
| `message` | string | Human-readable confirmation |
| `count` | integer | Number of sources persisted |

**Error responses:**

| Status | When |
|--------|------|
| `400` | Missing `filename` or `sources` fields |
| `401` | Invalid or missing `X-API-Key` |
| `404` | Frame `id` not found |

---

## 3. Save Anomalies for a Frame

Saves the list of classified anomalies for a previously registered frame. An empty list is valid and expected for frames with no anomalies.

### Request

```
POST /frames/{id}/anomalies
```

**URL parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string | Frame ID returned by `POST /frames` |

**Headers:**

```
X-API-Key: <api-key>
Content-Type: application/json
Accept: application/json
```

**Body:**

```json
{
  "filename": "frame_20240315_220134.fits",
  "anomalies": [
    {
      "anomaly_type": "ASTEROID",
      "ra": 202.489,
      "dec": 47.201,
      "magnitude": 17.8,
      "delta_mag": null,
      "mpc_designation": "2019 XY3",
      "ephemeris": {
        "predicted_ra": 202.491,
        "predicted_dec": 47.200,
        "predicted_mag": 17.9,
        "distance_au": 1.23,
        "angular_velocity_arcsec_per_hour": 45.2
      },
      "notes": "Matched MPC object within 3.2 arcsec"
    },
    {
      "anomaly_type": "SUPERNOVA_CANDIDATE",
      "ra": 202.502,
      "dec": 47.199,
      "magnitude": 16.1,
      "delta_mag": null,
      "mpc_designation": null,
      "ephemeris": null,
      "notes": "New source near galaxy (object_type='G'). Area covered by 14 previous frames."
    },
    {
      "anomaly_type": "VARIABLE_STAR",
      "ra": 202.478,
      "dec": 47.201,
      "magnitude": 16.85,
      "delta_mag": -0.82,
      "mpc_designation": null,
      "ephemeris": null,
      "notes": "Known variable star brightness change delta_mag=-0.820 (threshold 0.50). object_type='V*'."
    },
    {
      "anomaly_type": "UNKNOWN",
      "ra": 202.513,
      "dec": 47.188,
      "magnitude": 19.3,
      "delta_mag": null,
      "mpc_designation": null,
      "ephemeris": null,
      "notes": "Not found in Gaia DR3, Simbad, or MPC within 5.0 arcsec. Area covered by 8 previous frames."
    }
  ]
}
```

**Field descriptions:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `filename` | string | yes | FITS filename — included for logging and correlation |
| `anomalies` | array | yes | List of anomaly objects. An empty array `[]` is valid |
| `anomalies[].anomaly_type` | string | yes | Classification type — see table below |
| `anomalies[].ra` | float | yes | Source right ascension in decimal degrees |
| `anomalies[].dec` | float | yes | Source declination in decimal degrees |
| `anomalies[].magnitude` | float\|null | no | Observed magnitude |
| `anomalies[].delta_mag` | float\|null | no | Magnitude change vs. historical median. Negative = brighter than history |
| `anomalies[].mpc_designation` | string\|null | no | MPC designation for solar system objects, e.g. `"2019 XY3"`, `"C/2023 A3"` |
| `anomalies[].ephemeris` | object\|null | no | JPL Horizons predicted position; present only for `ASTEROID` and `COMET` types |
| `anomalies[].ephemeris.predicted_ra` | float | — | Predicted right ascension in decimal degrees |
| `anomalies[].ephemeris.predicted_dec` | float | — | Predicted declination in decimal degrees |
| `anomalies[].ephemeris.predicted_mag` | float\|null | — | Predicted apparent visual magnitude |
| `anomalies[].ephemeris.distance_au` | float\|null | — | Observer-centred distance in AU |
| `anomalies[].ephemeris.angular_velocity_arcsec_per_hour` | float\|null | — | Total angular velocity in arcseconds per hour |
| `anomalies[].notes` | string | no | Human-readable explanation of the classification |

**Anomaly type reference:**

| `anomaly_type` | Alert | Description |
|----------------|-------|-------------|
| `FIRST_OBSERVATION` | No | Sky area has no prior coverage — not sent to API |
| `KNOWN_CATALOG_NEW` | No | Source newly above detection threshold — not sent to API |
| `VARIABLE_STAR` | No | Known variable star with significant brightness change |
| `BINARY_STAR` | No | Known binary or eclipsing binary with brightness change |
| `ASTEROID` | No | Moving object matched in MPC asteroid catalog |
| `COMET` | No | Moving object matched in MPC comet catalog |
| `SUPERNOVA_CANDIDATE` | **YES** | New point source near a galaxy, not in any catalog |
| `MOVING_UNKNOWN` | **YES** | Moving object not found in MPC |
| `SPACE_DEBRIS` | **YES** | Fast linear trail not in MPC (highly elongated detection) |
| `UNKNOWN` | **YES** | New point source in a well-covered area, not in any catalog |

> **Note:** `FIRST_OBSERVATION` and `KNOWN_CATALOG_NEW` are handled internally by the pipeline and are never included in the anomalies payload sent to this endpoint.

### Response

**Status: `200 OK`** or **`201 Created`**

```json
{
  "message": "Anomalies saved successfully",
  "count": 4,
  "alerts": 2
}
```

| Field | Type | Description |
|-------|------|-------------|
| `message` | string | Human-readable confirmation |
| `count` | integer | Total number of anomalies persisted |
| `alerts` | integer | Number of alert-worthy anomalies (`SUPERNOVA_CANDIDATE`, `MOVING_UNKNOWN`, `SPACE_DEBRIS`, `UNKNOWN`) |

**Error responses:**

| Status | When |
|--------|------|
| `400` | Missing `filename` or `anomalies` fields |
| `401` | Invalid or missing `X-API-Key` |
| `404` | Frame `id` not found |

---

## 4. Get Historical Sources Near a Sky Position

Returns all previously detected sources within a cone radius of a given sky position, observed before a given time. Used by the anomaly detector to determine whether a source has been seen before.

### Request

```
GET /sources/near?ra={ra}&dec={dec}&radius_arcsec={radius}&before_time={iso8601}
```

**Headers:**

```
X-API-Key: <api-key>
Accept: application/json
```

**Query parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `ra` | float | yes | Right ascension of search centre in decimal degrees |
| `dec` | float | yes | Declination of search centre in decimal degrees |
| `radius_arcsec` | float | yes | Cone search radius in arcseconds. The pipeline uses `MATCH_CONE_ARCSEC` (default 5.0″) for stationary sources and `MOVING_CONE_ARCSEC` (default 30.0″) for moving object detection |
| `before_time` | string (ISO 8601) | yes | Only return sources from frames observed strictly before this timestamp, e.g. `2024-03-15T22:01:34Z` |

**Example:**

```
GET /sources/near?ra=202.461&dec=47.182&radius_arcsec=5.0&before_time=2024-03-15T22%3A01%3A34Z
```

### Response

**Status: `200 OK`**

```json
{
  "data": [
    {
      "ra": 202.4612,
      "dec": 47.1819,
      "mag": 14.21,
      "flux": 44850.0,
      "frame_id": "38",
      "obs_time": "2024-03-14T21:55:12Z"
    },
    {
      "ra": 202.4611,
      "dec": 47.1820,
      "mag": 14.24,
      "flux": 45100.0,
      "frame_id": "29",
      "obs_time": "2024-03-10T23:11:45Z"
    }
  ]
}
```

The response body may be either `{"data": [...]}` or a bare array `[...]` — the pipeline handles both formats.

| Field | Type | Description |
|-------|------|-------------|
| `data[].ra` | float | Source right ascension in decimal degrees |
| `data[].dec` | float | Source declination in decimal degrees |
| `data[].mag` | float\|null | Observed magnitude at the time of that frame |
| `data[].flux` | float\|null | Aperture flux in ADU |
| `data[].frame_id` | string | Frame in which this source was detected |
| `data[].obs_time` | string | Observation time of that frame (ISO 8601) |

Returns `{"data": []}` (empty array) when no prior detections exist — this is the normal case for a first observation.

**Error responses:**

| Status | When |
|--------|------|
| `400` | Missing or non-numeric `ra`, `dec`, or `radius_arcsec` |
| `401` | Invalid or missing `X-API-Key` |

---

## 5. Get Frames Covering a Sky Position

Returns all previously processed frames whose field of view covered a given sky position, observed before a given time. Used by the anomaly detector to establish whether the sky area has ever been imaged — a source absent from history is only an anomaly if the area was actually observed before.

### Request

```
GET /frames/covering?ra={ra}&dec={dec}&before_time={iso8601}
```

**Headers:**

```
X-API-Key: <api-key>
Accept: application/json
```

**Query parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `ra` | float | yes | Right ascension of sky point in decimal degrees |
| `dec` | float | yes | Declination of sky point in decimal degrees |
| `before_time` | string (ISO 8601) | yes | Only return frames observed strictly before this timestamp |

**Example:**

```
GET /frames/covering?ra=202.461&dec=47.182&before_time=2024-03-15T22%3A01%3A34Z
```

### Response

**Status: `200 OK`**

```json
{
  "data": [
    {
      "id": "38",
      "filename": "frame_20240314_215512.fits",
      "obs_time": "2024-03-14T21:55:12Z",
      "ra_center": 202.470,
      "dec_center": 47.195,
      "fov_deg": 1.25
    },
    {
      "id": "29",
      "filename": "frame_20240310_231145.fits",
      "obs_time": "2024-03-10T23:11:45Z",
      "ra_center": 202.469,
      "dec_center": 47.196,
      "fov_deg": 1.25
    }
  ]
}
```

The response body may be either `{"data": [...]}` or a bare array `[...]` — the pipeline handles both formats.

| Field | Type | Description |
|-------|------|-------------|
| `data[].id` | string | Frame ID |
| `data[].filename` | string | FITS filename |
| `data[].obs_time` | string | Observation time (ISO 8601) |
| `data[].ra_center` | float | Right ascension of frame centre in decimal degrees |
| `data[].dec_center` | float | Declination of frame centre in decimal degrees |
| `data[].fov_deg` | float | Field of view in degrees |

Returns `{"data": []}` (empty array) when no prior coverage exists — this is how `FIRST_OBSERVATION` is detected.

**Error responses:**

| Status | When |
|--------|------|
| `400` | Missing or non-numeric `ra` or `dec` |
| `401` | Invalid or missing `X-API-Key` |
