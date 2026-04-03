---
name: feedback_logging_extra_keys
description: Reserved keys in logging.LogRecord that must never be used in extra= dicts
type: feedback
---

Never use `"filename"` as a key in `extra=` dicts passed to logger calls. Python's `logging.LogRecord` already has a `filename` attribute (set to the source file name of the logging call), so passing `extra={"filename": ...}` raises `KeyError: "Attempt to overwrite 'filename' in LogRecord"` at runtime.

**Why:** `logging.Logger.makeRecord()` explicitly rejects any `extra` key that would overwrite a built-in `LogRecord` attribute. The built-in attributes include: `filename`, `name`, `msg`, `args`, `levelname`, `levelno`, `pathname`, `module`, `exc_info`, `exc_text`, `stack_info`, `lineno`, `funcName`, `created`, `msecs`, `relativeCreated`, `thread`, `threadName`, `processName`, `process`, `message`, `asctime`.

**How to apply:** When adding contextual fields to log records in pipeline modules, use prefixed key names like `fits_filename`, `fits_frame_id`, etc. to avoid any collision with the built-in LogRecord namespace.
