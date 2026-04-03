"""
api_client — HTTP client package for the observatory REST API.

Exports all public functions from client.py for convenient import:

    from api_client import post_frame, post_sources, post_anomalies
    from api_client import get_sources_near, get_frames_covering
"""

from api_client.client import (
    get_frames_covering,
    get_sources_near,
    post_anomalies,
    post_frame,
    post_sources,
)

__all__ = [
    "post_frame",
    "post_sources",
    "post_anomalies",
    "get_sources_near",
    "get_frames_covering",
]
