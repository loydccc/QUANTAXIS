#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""In-process shared state (single-process MVP).

Kept in a separate module to avoid circular imports between routers and app.
"""

from __future__ import annotations

import os
import threading

API_MAX_CONCURRENT = int(os.getenv("QUANTAXIS_API_MAX_CONCURRENT", "2"))

# Concurrency semaphore shared by /run and /signals/run
job_sem = threading.BoundedSemaphore(max(1, API_MAX_CONCURRENT))
