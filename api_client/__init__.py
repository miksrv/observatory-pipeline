"""
api_client — HTTP client package for the observatory REST API.

Split by REST resource, mirroring docs/API.md's own section grouping — see
that document for full request/response shapes:

  frames.py     (docs/API.md §1, 5, 7, 12, 13)
  sources.py    (docs/API.md §2, 4, 6, 8, 9, 11)
  anomalies.py  (docs/API.md §3)
  tasks.py      (docs/API.md §14)
  settings.py   (docs/API.md §16)
  _shared.py    internal HTTP infra (retry decorator, AsyncClient factory,
                batch-response normalization) used by every module above

Every public function is re-exported here, so the rest of this codebase
imports the whole package by name and calls straight into it:

    import api_client
    frame_id = await api_client.post_frame(frame_data)

Full list:

  post_frame(frame_data)                                    → str (frame_id)
  post_sources(frame_id, filename, sources)                 → list[str | None] | None
  post_anomalies(frame_id, filename, anomalies)             → None
  get_sources_near(ra, dec, radius_arcsec, before_time)     → list
  get_frames_covering(ra, dec, before_time)                 → list
  get_nearest_frame_before(object_name, before_time)        → dict | None
  get_sources_near_batch(positions, radius_arcsec, before_time)  → dict
  get_frames_covering_batch(positions, before_time)         → dict
  get_source_track(source_id)                               → list
  get_source_tracks_batch(source_ids)                       → dict
  upload_source_chart(source_id, png_bytes, style, frame_count)  → bool

  get_frames(object_name, date_from, date_to, limit, offset) → list
  get_frame(frame_id)                                        → dict | None
  get_frame_sources(frame_id)                                → list

  create_task(task_type, items, scope, parent_task_id)       → dict | None
  get_tasks(status, task_type, object_name, limit, order)    → list
  get_task(task_id)                                          → dict | None
  update_task(task_id, status, error)                        → dict | None
  post_task_items_progress(task_id, items)                   → dict | None
  upload_task_item_chart(task_id, item_id, png_bytes, style, frame_count)  → bool

  get_settings()                                            → dict[str, str] | None

`httpx` is imported here (unused directly) so that
``unittest.mock.patch("api_client.httpx.AsyncClient", ...)`` — the mocking
strategy tests/test_api_client.py uses — resolves without needing to know
which specific submodule actually calls it.
"""

from __future__ import annotations

import httpx  # noqa: F401 — see docstring above; patch() target resolution needs this attribute.

from .anomalies import post_anomalies
from .frames import (
    get_frame,
    get_frames,
    get_frames_covering,
    get_frames_covering_batch,
    get_frame_sources,
    get_nearest_frame_before,
    post_frame,
)
from .settings import get_settings
from .sources import (
    get_source_track,
    get_source_tracks_batch,
    get_sources_near,
    get_sources_near_batch,
    post_sources,
    upload_source_chart,
)
from .tasks import (
    create_task,
    get_task,
    get_tasks,
    post_task_items_progress,
    update_task,
    upload_task_item_chart,
)

__all__ = [
    "post_frame",
    "post_sources",
    "post_anomalies",
    "get_sources_near",
    "get_frames_covering",
    "get_nearest_frame_before",
    "get_sources_near_batch",
    "get_frames_covering_batch",
    "get_source_track",
    "get_source_tracks_batch",
    "upload_source_chart",
    "get_frames",
    "get_frame",
    "get_frame_sources",
    "create_task",
    "get_tasks",
    "get_task",
    "update_task",
    "post_task_items_progress",
    "upload_task_item_chart",
    "get_settings",
]
