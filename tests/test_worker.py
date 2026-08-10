"""
tests/test_worker.py — Unit tests for worker.py.

All external dependencies (pipeline.py's stage functions, api_client) are
mocked. No real FITS I/O or network calls occur.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import worker


# ---------------------------------------------------------------------------
# _handle_analyze_item
# ---------------------------------------------------------------------------


class TestHandleAnalyzeItem:
    async def test_success_reports_done_with_frame_id(self, monkeypatch):
        monkeypatch.setattr(
            worker.pipeline, "analyze_frame",
            AsyncMock(return_value={"frame_id": "frame-1"}),
        )
        item = {"id": "item-1", "filename": "/fits/incoming/a.fits"}

        result = await worker._handle_analyze_item(item)

        assert result == {"item_id": "item-1", "status": "DONE", "frame_id": "frame-1"}

    async def test_none_result_is_still_done_without_frame_id(self, monkeypatch):
        """analyze_frame() returning None (QC-rejected, calibration frame) is
        a normal successful outcome, not a failure."""
        monkeypatch.setattr(worker.pipeline, "analyze_frame", AsyncMock(return_value=None))
        item = {"id": "item-1", "filename": "/fits/incoming/a.fits"}

        result = await worker._handle_analyze_item(item)

        assert result == {"item_id": "item-1", "status": "DONE"}

    async def test_exception_reports_failed(self, monkeypatch):
        monkeypatch.setattr(
            worker.pipeline, "analyze_frame",
            AsyncMock(side_effect=RuntimeError("astap crashed")),
        )
        item = {"id": "item-1", "filename": "/fits/incoming/a.fits"}

        result = await worker._handle_analyze_item(item)

        assert result["item_id"] == "item-1"
        assert result["status"] == "FAILED"
        assert "astap crashed" in result["error"]

    async def test_missing_filename_reports_failed_without_calling_pipeline(self, monkeypatch):
        analyze_mock = AsyncMock()
        monkeypatch.setattr(worker.pipeline, "analyze_frame", analyze_mock)
        item = {"id": "item-1"}

        result = await worker._handle_analyze_item(item)

        assert result["status"] == "FAILED"
        analyze_mock.assert_not_called()


# ---------------------------------------------------------------------------
# _run_detect_task
# ---------------------------------------------------------------------------


class TestRunDetectTask:
    async def test_processes_all_items_and_reports_progress(self, monkeypatch):
        """All frame items are processed sequentially, each reporting progress."""

        async def fake_detect(frame_id):
            return [
                {"anomaly_type": "UNKNOWN", "source_id": "src-a", "_designation": None},
            ]

        monkeypatch.setattr(worker.pipeline, "detect_anomalies_for_frame_id", AsyncMock(side_effect=fake_detect))
        post_progress_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "post_task_items_progress", post_progress_mock)

        task = {"id": "detect-task-1", "scope_object": "M51"}
        items = [
            {"id": "item-1", "frame_id": "frame-1"},
            {"id": "item-2", "frame_id": "frame-2"},
        ]

        await worker._run_detect_task(task, items)

        # Progress reported immediately after EACH item, not batched into one
        # call at the end — otherwise task_items/tasks.completed_items would
        # only ever update once the whole task finishes, with no live
        # visibility while it's still running.
        assert post_progress_mock.call_count == 2
        reported = {}
        for call in post_progress_mock.call_args_list:
            (_, items_arg) = call.args
            assert len(items_arg) == 1  # one item per call
            reported[items_arg[0]["item_id"]] = items_arg[0]["status"]
        assert reported == {"item-1": "DONE", "item-2": "DONE"}

    async def test_no_automatic_charts_task_created(self, monkeypatch):
        """DETECT_ANOMALIES must NOT auto-create a follow-up GENERATE_CHARTS task.
        The operator creates chart tasks manually from the UI."""

        async def fake_detect(frame_id):
            return [{"anomaly_type": "UNKNOWN", "source_id": "src-a", "_designation": None}]

        monkeypatch.setattr(worker.pipeline, "detect_anomalies_for_frame_id", AsyncMock(side_effect=fake_detect))
        monkeypatch.setattr(worker.api_client, "post_task_items_progress", AsyncMock())
        create_task_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "create_task", create_task_mock)

        task = {"id": "detect-task-1", "scope_object": "M51"}
        items = [{"id": "item-1", "frame_id": "frame-1"}]

        await worker._run_detect_task(task, items)

        # No follow-up task should be created — charts are operator-driven now.
        create_task_mock.assert_not_called()

    async def test_item_failure_does_not_block_others(self, monkeypatch):
        async def fake_detect(frame_id):
            if frame_id == "frame-bad":
                raise RuntimeError("API down")
            return [{"anomaly_type": "UNKNOWN", "source_id": "src-ok", "_designation": None}]

        monkeypatch.setattr(worker.pipeline, "detect_anomalies_for_frame_id", AsyncMock(side_effect=fake_detect))
        post_progress_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "post_task_items_progress", post_progress_mock)

        task = {"id": "detect-task-1"}
        items = [
            {"id": "item-1", "frame_id": "frame-bad"},
            {"id": "item-2", "frame_id": "frame-ok"},
        ]

        await worker._run_detect_task(task, items)

        by_id = {}
        for call in post_progress_mock.call_args_list:
            entry = call.args[1][0]
            by_id[entry["item_id"]] = entry
        assert by_id["item-1"]["status"] == "FAILED"
        assert by_id["item-2"]["status"] == "DONE"



# ---------------------------------------------------------------------------
# _run_charts_task
# ---------------------------------------------------------------------------


class TestRunChartsTask:
    async def test_batches_all_items_into_one_call(self, monkeypatch):
        generate_mock = AsyncMock(return_value={"src-a": True, "src-b": False})
        monkeypatch.setattr(worker.pipeline, "generate_charts_for_source_ids", generate_mock)
        monkeypatch.setattr(worker.api_client, "post_task_items_progress", AsyncMock())

        items = [
            {"id": "item-1", "source_id": "src-a", "payload": {"anomaly_type": "UNKNOWN", "designation": None}},
            {"id": "item-2", "source_id": "src-b", "payload": {"anomaly_type": "ASTEROID", "designation": "Vesta"}},
        ]

        await worker._run_charts_task({"id": "chart-task-1"}, items)

        # One call covering both source_ids at once — not one call per item.
        generate_mock.assert_called_once()
        anomaly_type_by_source_id, designation_by_source_id = generate_mock.call_args.args
        assert anomaly_type_by_source_id == {"src-a": "UNKNOWN", "src-b": "ASTEROID"}
        assert designation_by_source_id == {"src-b": "Vesta"}

        progress = worker.api_client.post_task_items_progress.call_args.args[1]
        by_id = {p["item_id"]: p["status"] for p in progress}
        assert by_id == {"item-1": "DONE", "item-2": "FAILED"}

    async def test_malformed_item_fails_without_calling_pipeline(self, monkeypatch):
        generate_mock = AsyncMock()
        monkeypatch.setattr(worker.pipeline, "generate_charts_for_source_ids", generate_mock)
        monkeypatch.setattr(worker.api_client, "post_task_items_progress", AsyncMock())

        items = [{"id": "item-1", "source_id": None, "payload": {}}]

        await worker._run_charts_task({"id": "chart-task-1"}, items)

        generate_mock.assert_not_called()
        progress = worker.api_client.post_task_items_progress.call_args.args[1]
        assert len(progress) == 1
        assert progress[0]["item_id"] == "item-1"
        assert progress[0]["status"] == "FAILED"


# ---------------------------------------------------------------------------
# _run_preview_task
# ---------------------------------------------------------------------------


class TestRunPreviewTask:
    async def test_success_reports_done_with_result_payload(self, monkeypatch):
        preview_mock = AsyncMock(return_value={
            "matched": 40, "total": 55, "quality_flag": "OK", "chart_uploaded": True,
        })
        monkeypatch.setattr(worker.pipeline, "preview_catalog_match", preview_mock)
        post_progress_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "post_task_items_progress", post_progress_mock)

        task = {"id": "t1"}
        items = [{"id": "item-1", "filename": "/fits/incoming/frame.fits"}]

        await worker._run_preview_task(task, items)

        # task_id AND item_id are both passed through — preview_catalog_match()
        # needs both to build the chart-upload URL.
        preview_mock.assert_called_once_with("/fits/incoming/frame.fits", "t1", "item-1")

        post_progress_mock.assert_called_once()
        reported = post_progress_mock.call_args.args[1][0]
        assert reported["item_id"] == "item-1"
        assert reported["status"] == "DONE"
        assert reported["payload"] == {
            "matched": 40, "total": 55, "quality_flag": "OK", "chart_uploaded": True,
        }

    async def test_exception_reports_failed(self, monkeypatch):
        monkeypatch.setattr(
            worker.pipeline, "preview_catalog_match",
            AsyncMock(side_effect=RuntimeError("Astrometry failed for frame.fits")),
        )
        post_progress_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "post_task_items_progress", post_progress_mock)

        await worker._run_preview_task({"id": "t1"}, [{"id": "item-1", "filename": "/x.fits"}])

        reported = post_progress_mock.call_args.args[1][0]
        assert reported["status"] == "FAILED"
        assert "Astrometry failed" in reported["error"]

    async def test_missing_filename_reports_failed_without_calling_pipeline(self, monkeypatch):
        preview_mock = AsyncMock()
        monkeypatch.setattr(worker.pipeline, "preview_catalog_match", preview_mock)
        post_progress_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "post_task_items_progress", post_progress_mock)

        await worker._run_preview_task({"id": "t1"}, [{"id": "item-1"}])

        preview_mock.assert_not_called()
        assert post_progress_mock.call_args.args[1][0]["status"] == "FAILED"

    async def test_multiple_items_reported_immediately_per_item_not_batched(self, monkeypatch):
        preview_mock = AsyncMock(return_value={
            "matched": 1, "total": 1, "quality_flag": "OK", "chart_uploaded": True,
        })
        monkeypatch.setattr(worker.pipeline, "preview_catalog_match", preview_mock)
        post_progress_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "post_task_items_progress", post_progress_mock)

        items = [
            {"id": "item-1", "filename": "/a.fits"},
            {"id": "item-2", "filename": "/b.fits"},
        ]
        await worker._run_preview_task({"id": "t1"}, items)

        assert post_progress_mock.call_count == 2
        assert preview_mock.call_count == 2


# ---------------------------------------------------------------------------
# _run_restart_task
# ---------------------------------------------------------------------------


class TestRunRestartTask:
    async def test_raises_restart_requested(self):
        with pytest.raises(worker.RestartRequested):
            await worker._run_restart_task({"id": "t1"}, [])


# ---------------------------------------------------------------------------
# _process_one_task — dispatch
# ---------------------------------------------------------------------------


class TestProcessOneTask:
    async def test_dispatches_to_matching_handler(self, monkeypatch):
        detail = {
            "task": {"id": "t1", "type": "ANALYZE"},
            "items": [{"id": "item-1", "status": "PENDING", "filename": "/x.fits"}],
        }
        monkeypatch.setattr(worker.api_client, "get_task", AsyncMock(return_value=detail))
        monkeypatch.setattr(worker.api_client, "update_task", AsyncMock())
        handler_mock = AsyncMock()
        monkeypatch.setattr(worker, "_HANDLERS", {**worker._HANDLERS, "ANALYZE": handler_mock})

        await worker._process_one_task({"id": "t1"})

        worker.api_client.update_task.assert_called_once_with("t1", "RUNNING")
        handler_mock.assert_called_once()

    async def test_only_pending_items_are_passed_to_handler(self, monkeypatch):
        detail = {
            "task": {"id": "t1", "type": "ANALYZE"},
            "items": [
                {"id": "item-1", "status": "DONE", "filename": "/a.fits"},
                {"id": "item-2", "status": "PENDING", "filename": "/b.fits"},
            ],
        }
        monkeypatch.setattr(worker.api_client, "get_task", AsyncMock(return_value=detail))
        monkeypatch.setattr(worker.api_client, "update_task", AsyncMock())
        handler_mock = AsyncMock()
        monkeypatch.setattr(worker, "_HANDLERS", {**worker._HANDLERS, "ANALYZE": handler_mock})

        await worker._process_one_task({"id": "t1"})

        passed_items = handler_mock.call_args.args[1]
        assert [i["id"] for i in passed_items] == ["item-2"]

    async def test_no_pending_items_skips_handler_and_running_transition(self, monkeypatch):
        detail = {
            "task": {"id": "t1", "type": "ANALYZE"},
            "items": [{"id": "item-1", "status": "DONE", "filename": "/a.fits"}],
        }
        monkeypatch.setattr(worker.api_client, "get_task", AsyncMock(return_value=detail))
        update_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "update_task", update_mock)

        await worker._process_one_task({"id": "t1"})

        update_mock.assert_not_called()

    async def test_unknown_task_type_marks_failed(self, monkeypatch):
        detail = {
            "task": {"id": "t1", "type": "SOMETHING_NEW"},
            "items": [{"id": "item-1", "status": "PENDING"}],
        }
        monkeypatch.setattr(worker.api_client, "get_task", AsyncMock(return_value=detail))
        update_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "update_task", update_mock)

        await worker._process_one_task({"id": "t1"})

        update_mock.assert_called_once_with("t1", "FAILED", error="Unknown task type: SOMETHING_NEW")

    async def test_handler_exception_marks_task_failed(self, monkeypatch):
        detail = {
            "task": {"id": "t1", "type": "ANALYZE"},
            "items": [{"id": "item-1", "status": "PENDING", "filename": "/a.fits"}],
        }
        monkeypatch.setattr(worker.api_client, "get_task", AsyncMock(return_value=detail))
        update_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "update_task", update_mock)
        monkeypatch.setattr(
            worker, "_HANDLERS",
            {**worker._HANDLERS, "ANALYZE": AsyncMock(side_effect=RuntimeError("boom"))},
        )

        await worker._process_one_task({"id": "t1"})

        # First call transitions to RUNNING, second reports FAILED.
        assert update_mock.call_args_list[0].args == ("t1", "RUNNING")
        assert update_mock.call_args_list[1].args == ("t1", "FAILED")

    async def test_missing_task_detail_is_skipped_quietly(self, monkeypatch):
        monkeypatch.setattr(worker.api_client, "get_task", AsyncMock(return_value=None))
        update_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "update_task", update_mock)

        await worker._process_one_task({"id": "t1"})

        update_mock.assert_not_called()

    async def test_restart_task_marks_completed_and_re_raises(self, monkeypatch):
        """RESTART is a signal task (no items). _process_one_task() must mark it
        COMPLETED and re-raise RestartRequested so run_forever() can exit."""
        detail = {
            "task": {"id": "t1", "type": "RESTART"},
            "items": [],  # signal task — no items
        }
        monkeypatch.setattr(worker.api_client, "get_task", AsyncMock(return_value=detail))
        update_mock = AsyncMock()
        monkeypatch.setattr(worker.api_client, "update_task", update_mock)

        with pytest.raises(worker.RestartRequested):
            await worker._process_one_task({"id": "t1"})

        # First call: RUNNING, second call: COMPLETED.
        assert update_mock.call_count == 2
        assert update_mock.call_args_list[0].args == ("t1", "RUNNING")
        assert update_mock.call_args_list[1].args == ("t1", "COMPLETED")


# ---------------------------------------------------------------------------
# run_forever — polling / backoff
# ---------------------------------------------------------------------------


class TestRunForever:
    async def test_backs_off_on_consecutive_empty_polls_and_resets_on_work(self, monkeypatch):
        monkeypatch.setattr(worker.config, "TASK_POLL_INTERVAL_SEC", 1.0)
        monkeypatch.setattr(worker.config, "TASK_POLL_BACKOFF_MAX_SEC", 4.0)

        # Empty, empty, empty (backoff 1 -> 2 -> 4, capped), then one task found
        # (resets to 1). _process_one_task() is called OUTSIDE run_forever()'s
        # try/except (only the get_tasks() call itself is guarded), so making
        # it raise is what actually breaks out of the otherwise-infinite loop
        # — raising from get_tasks() instead would just be swallowed and the
        # loop would spin forever, which is what hung the first version of
        # this test.
        get_tasks_mock = AsyncMock(side_effect=[[], [], [], [{"id": "t1"}]])
        monkeypatch.setattr(worker.api_client, "get_tasks", get_tasks_mock)
        monkeypatch.setattr(worker, "_process_one_task", AsyncMock(side_effect=RuntimeError("stop test")))
        sleep_mock = AsyncMock()
        monkeypatch.setattr(worker.asyncio, "sleep", sleep_mock)

        with pytest.raises(RuntimeError, match="stop test"):
            await worker.run_forever()

        assert [call.args[0] for call in sleep_mock.call_args_list] == [1.0, 2.0, 4.0]
        # order=asc — oldest PENDING task claimed first, not newest.
        get_tasks_mock.assert_any_call(status="PENDING", limit=1, order="asc")
